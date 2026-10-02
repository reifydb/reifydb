// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use arrow_array::RecordBatch;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	ringbuffer::EncodedRingBufferRowBuilder,
	shape::RowShape,
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		query::column_not_found,
	},
	expression::Expression,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::{RingBuffer, RingBufferMetadata},
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedRingBuffer},
	},
	partition::partition_of,
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{nodes::InsertRingBufferNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
#[cfg(reifydb_assertions)]
use reifydb_value::value::canonical::assert_canonical_floats;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{
		Value, identity::IdentityId, partition::Partition, row_number::RowNumber, system_columns::user_columns,
	},
};
use tracing::instrument;

use super::{
	coerce::InputFragments,
	columns::{ColumnPipeline, input_views, intern_dictionary_columns},
	context::RingBufferTarget,
	partition::{
		compute_partition_col_indices, ensure_partition_metadata, partition_values,
		save_all_partition_metadata, select_oldest_for_partition, update_metadata_after_insert,
	},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::ringbuffer::RingBufferOperations,
	vm::{
		instruction::dml::time::{EventColumn, populator_index, resolve_time},
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

#[instrument(name = "mutate::ringbuffer::insert", level = "trace", skip_all)]
pub(crate) fn insert_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertRingBufferNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<RecordBatch> {
	let InsertRingBufferNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, ringbuffer, shape) = resolve_insert_ringbuffer_target_and_shape(services, txn, &target)?;
	let target_data = RingBufferTarget {
		namespace: &namespace,
		ringbuffer: &ringbuffer,
	};
	let context = build_insert_ringbuffer_query_context(services, &target_data, &params, symbols, txn.identity());
	let fragments = InputFragments::of(&input);
	let mut input_node = compile_and_initialize_input(*input, txn, &context)?;

	let mut partition_metadata_cache: HashMap<Vec<Value>, RingBufferMetadata> = HashMap::new();
	let (inserted_count, returned_rows) = drive_ringbuffer_insert(
		services,
		txn,
		symbols,
		&target_data,
		&shape,
		&context,
		input_node.as_mut(),
		&fragments,
		returning.is_some(),
		&mut partition_metadata_cache,
	)?;

	finalize_ringbuffer_insert(
		services,
		txn,
		&target_data,
		&shape,
		symbols,
		&returning,
		&partition_metadata_cache,
		&returned_rows,
		inserted_count,
	)
}

#[inline]
fn compile_and_initialize_input<'a>(
	input: QueryPlan,
	txn: &mut Transaction<'a>,
	context: &Arc<QueryContext>,
) -> Result<Box<dyn QueryNode>> {
	let mut input_node = compile(input, txn, context.clone());
	input_node.initialize(txn, context)?;
	Ok(input_node)
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn drive_ringbuffer_insert(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
	target_data: &RingBufferTarget<'_>,
	shape: &RowShape,
	context: &Arc<QueryContext>,
	input_node: &mut dyn QueryNode,
	fragments: &InputFragments,
	has_returning: bool,
	partition_metadata_cache: &mut HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<(u64, Vec<(RowNumber, EncodedBytes)>)> {
	let namespace = target_data.namespace;
	let ringbuffer = target_data.ringbuffer;
	let partition_col_indices = compute_partition_col_indices(ringbuffer);
	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let pipeline = ColumnPipeline {
		columns: &ringbuffer.columns,
		sequences: None,
		series_key: None,
		fragments,
		context,
	};
	let populator = populator_index(&ringbuffer.time, shape);

	let mut mutable_context = (**context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		reifydb_assertions! {
			assert_canonical_floats(&columns, "ringbuffer insert");
		}
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&ringbuffer.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;
		if let Some((unknown, _)) = user_columns(&columns)
			.find(|(field, _)| !ringbuffer.columns.iter().any(|c| &c.name == field.name()))
		{
			return_error!(column_not_found(fragments.column(unknown.name())));
		}

		let row_count = columns.num_rows();
		let inputs = input_views(&columns, &ringbuffer.columns)?;
		let mut batches = [pipeline.cast_target_columns(&inputs, row_count, None)?];
		intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut batches)?;
		let mut built: Vec<EncodedRingBufferRowBuilder> =
			(0..row_count).map(|_| shape.allocate_ringbuffer()).collect();
		batches[0].write(shape, &mut built)?;
		let partition_keys = partition_values(&batches[0], &inputs, &partition_col_indices)?;

		let now = services.runtime_context.clock.now();
		let event = EventColumn::new(populator.map(|index| batches[0].view(index)).transpose()?);
		let mut rows = Vec::with_capacity(row_count);
		for (index, (mut row, partition_key)) in built.into_iter().zip(partition_keys).enumerate() {
			row.set_timestamps(now, now);
			if let Some(time) = event.at(index).map_or_else(
				|| resolve_time(&ringbuffer.name, &ringbuffer.time, shape, &row, now),
				|time| Ok(Some(time)),
			)? {
				row.set_time(time);
			}
			let partition = if partition_col_indices.is_empty() {
				None
			} else {
				Some(partition_of(&ringbuffer.columns, &ringbuffer.partition_by, &partition_key))
			};
			rows.push((row.freeze_bytes(), partition_key, partition));
		}

		inserted_count += insert_ringbuffer_chunks(
			&services.catalog,
			txn,
			ringbuffer,
			shape,
			&rows,
			partition_metadata_cache,
			has_returning.then_some(&mut returned_rows),
		)?;
	}

	Ok((inserted_count, returned_rows))
}

pub(crate) fn insert_ringbuffer_chunks(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	rows: &[(EncodedBytes, Vec<Value>, Option<Partition>)],
	cache: &mut HashMap<Vec<Value>, RingBufferMetadata>,
	mut returned: Option<&mut Vec<(RowNumber, EncodedBytes)>>,
) -> Result<u64> {
	let mut inserted_count = 0u64;
	let mut start = 0;
	while start < rows.len() {
		let mut held: HashMap<&[Value], u64> = HashMap::new();
		let mut end = start;
		while end < rows.len() {
			let count = held.entry(rows[end].1.as_slice()).or_default();
			if *count >= ringbuffer.capacity && end > start {
				break;
			}
			*count += 1;
			end += 1;
		}

		let mut chosen = HashSet::new();
		let mut pending = HashSet::new();
		let mut victims = Vec::new();
		let mut victim_partitions = Vec::new();
		let mut ids = Vec::with_capacity(end - start);
		let mut partitions = Vec::with_capacity(end - start);
		let mut encoded = Vec::with_capacity(end - start);
		for (row, partition_key, partition) in &rows[start..end] {
			ensure_partition_metadata(catalog, txn, ringbuffer, partition_key, cache)?;
			let current_metadata = cache.get_mut(partition_key).unwrap();

			if current_metadata.is_full(ringbuffer.capacity)
				&& let Some(victim) = select_oldest_for_partition(
					txn,
					ringbuffer,
					*partition,
					current_metadata,
					&chosen,
					&pending,
				)? {
				chosen.insert(victim);
				victims.push(victim);
				victim_partitions.extend(*partition);
			}

			let row_number = catalog.next_row_number_for_ringbuffer(txn, ringbuffer.id)?;
			pending.insert(row_number);
			update_metadata_after_insert(current_metadata, row_number);
			ids.push(row_number);
			partitions.extend(*partition);
			encoded.push(row.clone());
		}

		if !victims.is_empty() {
			txn.remove_from_ringbuffer(ringbuffer, &victim_partitions, &victims)?;
		}
		let stored = txn.insert_ringbuffer(ringbuffer, shape, &partitions, &ids, &encoded)?;
		if let Some(returned) = returned.as_mut() {
			returned.extend(ids.iter().copied().zip(stored));
		}
		inserted_count += (end - start) as u64;
		start = end;
	}
	Ok(inserted_count)
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn finalize_ringbuffer_insert(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target_data: &RingBufferTarget<'_>,
	shape: &RowShape,
	symbols: &SymbolTable,
	returning: &Option<Vec<Expression>>,
	partition_metadata_cache: &HashMap<Vec<Value>, RingBufferMetadata>,
	returned_rows: &[(RowNumber, EncodedBytes)],
	inserted_count: u64,
) -> Result<RecordBatch> {
	let ringbuffer = target_data.ringbuffer;
	save_all_partition_metadata(&services.catalog, txn, ringbuffer, partition_metadata_cache)?;

	reifydb_assertions! {
		let returning_rows_match = returning.is_none() || returned_rows.len() as u64 == inserted_count;
		assert!(
			returning_rows_match,
			"ringbuffer insert with a RETURNING clause must capture one stored row per inserted row \
			 so the returned batch reflects every insert; captured {} rows but inserted {}",
			returned_rows.len(),
			inserted_count
		);
	}

	if let Some(returning_exprs) = returning {
		let columns = decode_rows_to_columns(shape, returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &ringbuffer.columns, columns)?;
		let columns = with_absent_pre_image(columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	insert_ringbuffer_result(target_data.namespace.name(), &ringbuffer.name, inserted_count)
}

#[inline]
fn resolve_insert_ringbuffer_target_and_shape(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedRingBuffer,
) -> Result<(Namespace, RingBuffer, RowShape)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let ringbuffer_name = target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id(), ringbuffer_name)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;
	Ok((namespace, ringbuffer, shape))
}

#[inline]
fn build_insert_ringbuffer_query_context(
	services: &Arc<Services>,
	target: &RingBufferTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
	identity: IdentityId,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let rb_ident = Fragment::internal(target.ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, target.ringbuffer.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::RingBuffer(resolved_rb)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	})
}

#[inline]
fn insert_ringbuffer_result(namespace: &str, ringbuffer: &str, inserted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, sync::Arc};

	use arrow_array::{ArrayRef, Float64Array, RecordBatch};
	use reifydb_codec::row::shape::{RowFamily, RowShape};
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			id::{NamespaceId, RingBufferId},
			namespace::Namespace,
			ringbuffer::RingBuffer,
		},
		value::column::headers::ColumnHeaders,
	};
	use reifydb_evaluate::stack::SymbolTable;
	use reifydb_rql::{nodes::InlineDataNode, query::QueryPlan};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		params::Params,
		value::{identity::IdentityId, value_type::ValueType},
	};

	use super::{InputFragments, RingBufferTarget, build_insert_ringbuffer_query_context, drive_ringbuffer_insert};
	use crate::{
		Result,
		vm::{
			services::Services,
			volcano::query::{QueryContext, QueryNode},
		},
	};

	struct NegativeZeroNode;

	impl QueryNode for NegativeZeroNode {
		fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
			Ok(())
		}

		fn next<'a>(
			&mut self,
			_rx: &mut Transaction<'a>,
			_ctx: &mut QueryContext,
		) -> Result<Option<RecordBatch>> {
			let column: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
			Ok(Some(RecordBatch::try_from_iter([("c", column)]).unwrap()))
		}

		fn headers(&self) -> Option<ColumnHeaders> {
			None
		}
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn test_drive_with_negative_zero_panics() {
		// The insert root skips the Box check, so the loop must check or a raw -0.0 is stored.
		let services = Services::testing();
		let mut txn = create_test_admin_transaction();
		let namespace = Namespace::Local {
			id: NamespaceId(1),
			name: "app".to_string(),
			local_name: "app".to_string(),
			parent_id: NamespaceId(0),
		};
		let ringbuffer = RingBuffer {
			id: RingBufferId(1),
			namespace: NamespaceId(1),
			name: "rb".to_string(),
			columns: vec![],
			capacity: 1,
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::None,
		};
		let target = RingBufferTarget {
			namespace: &namespace,
			ringbuffer: &ringbuffer,
		};
		let shape = RowShape::testing(RowFamily::RingBuffer, &[ValueType::Float8]);
		let symbols = SymbolTable::new();
		let context = build_insert_ringbuffer_query_context(
			&services,
			&target,
			&Params::default(),
			&symbols,
			IdentityId::system(),
		);
		let fragments = InputFragments::of(&QueryPlan::InlineData(InlineDataNode {
			rows: vec![],
		}));
		let _ = drive_ringbuffer_insert(
			&services,
			&mut Transaction::Admin(&mut txn),
			&symbols,
			&target,
			&shape,
			&context,
			&mut NegativeZeroNode,
			&fragments,
			false,
			&mut HashMap::new(),
		);
	}
}
