// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	ringbuffer::{EncodedRingBufferRow, EncodedRingBufferRowBuilder},
	shape::RowShape,
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
		query::column_not_found,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::{PartitionedMetadata, RingBuffer},
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedRingBuffer},
	},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey},
	},
	partition::{PartitionError, partition_col_indices, partition_of, partition_values},
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::UpdateRingBufferNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{self, row_numbers, user_columns},
	},
};

use super::{
	coerce::InputFragments,
	columns::{ColumnPipeline, input_views, intern_dictionary_columns},
	context::RingBufferTarget,
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::ringbuffer::RingBufferOperations,
	vm::{
		instruction::dml::time::resolve_time_for_update,
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

pub(crate) fn update_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateRingBufferNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<RecordBatch> {
	let UpdateRingBufferNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, ringbuffer) = resolve_update_ringbuffer_target(services, txn, &target)?;
	let partitions = services.catalog.list_ringbuffer_partitions(txn, &ringbuffer)?;
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;
	let target_data = RingBufferTarget {
		namespace: &namespace,
		ringbuffer: &ringbuffer,
	};
	let context = build_update_ringbuffer_query_context(services, &target_data, &params, symbols, txn.identity());

	let fragments = InputFragments::of(&input);
	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let mut updated_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let mut pre_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let has_returning = returning.is_some();
	let pipeline = ColumnPipeline {
		columns: &ringbuffer.columns,
		sequences: None,
		series_key: None,
		fragments: &fragments,
		context: &context,
	};

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		if columns.num_rows() == 0 {
			continue;
		}
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&ringbuffer.name,
			DataOp::Update,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;
		if let Some((unknown, _)) = user_columns(&columns)
			.find(|(field, _)| !ringbuffer.columns.iter().any(|c| &c.name == field.name()))
		{
			return_error!(column_not_found(fragments.column(unknown.name())));
		}
		if row_numbers(&columns)?.is_empty() {
			return_error!(engine::missing_row_number_column());
		}
		enforce_old_row_policies(services, symbols, txn, &target_data, &shape, &columns)?;
		let row_numbers = row_numbers(&columns)?;
		let sidecar_partitions = system_columns::partitions(&columns)?;
		let inputs = input_views(&columns, &ringbuffer.columns)?;
		let mut batches = [pipeline.cast_target_columns(&inputs, columns.num_rows(), None)?];
		intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut batches)?;
		let mut built: Vec<EncodedRingBufferRowBuilder> =
			(0..columns.num_rows()).map(|_| shape.allocate_ringbuffer()).collect();
		batches[0].write(&shape, &mut built)?;

		let mut ids = Vec::with_capacity(row_numbers.len());
		let mut update_partitions = Vec::new();
		let mut rows = Vec::with_capacity(row_numbers.len());
		for (row_idx, (mut builder, &row_number)) in built.into_iter().zip(row_numbers.iter()).enumerate() {
			let partition = if sidecar_partitions.is_empty() {
				None
			} else {
				Some(sidecar_partitions[row_idx])
			};
			let old_row_key = match partition {
				None => TaggedKey::from(RowKey::new(ringbuffer.id, row_number)),
				Some(p) => TaggedKey::from(PartitionedRowKey::new(ringbuffer.id, p, row_number)),
			};
			let old_row = txn.get(&old_row_key)?.expect("bytes must exist for update").bytes;
			let pre_row = old_row.clone();
			let old_row = EncodedRingBufferRow::view(&old_row);
			let old_created_at = old_row.created_at();
			let old_time = old_row.time();
			let now = services.runtime_context.clock.now();
			builder.set_timestamps(old_created_at, now);
			if let Some(time) = resolve_time_for_update(
				&ringbuffer.name,
				&ringbuffer.columns,
				&ringbuffer.time,
				&shape,
				builder.as_slice(),
				old_time,
			)? {
				builder.set_time(time);
			}
			let row = builder.freeze_bytes();

			if !row_belongs_to_any_partition(&partitions, row_number) {
				continue;
			}

			if !ringbuffer.partition_by.is_empty() {
				let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
				let new_partition = partition_of(
					&ringbuffer.columns,
					&ringbuffer.partition_by,
					&partition_values(&shape, &row, &indices),
				);
				if Some(new_partition) != partition {
					return Err(PartitionError::ImmutablePartitionColumn {
						object: ObjectId::ringbuffer(ringbuffer.id),
					}
					.into());
				}
			}

			if has_returning {
				pre_rows.push((row_number, pre_row));
			}
			ids.push(row_number);
			update_partitions.extend(partition);
			rows.push(row);
		}

		let stored = txn.update_ringbuffer(&ringbuffer, &update_partitions, &ids, &rows)?;
		if has_returning {
			returned_rows.extend(ids.iter().copied().zip(stored));
		}
		updated_count += ids.len() as u64;
	}

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &ringbuffer.columns, columns)?;
		let pre_columns = decode_rows_to_columns(&shape, &pre_rows)?;
		let pre_columns = decode_returning_dictionaries(services, txn, &ringbuffer.columns, pre_columns)?;
		let columns = with_pre_image(columns, &pre_columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	update_ringbuffer_result(namespace.name(), &ringbuffer.name, updated_count)
}

#[inline]
fn resolve_update_ringbuffer_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedRingBuffer,
) -> Result<(Namespace, RingBuffer)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let ringbuffer_name = target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id(), ringbuffer_name)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};
	Ok((namespace, ringbuffer))
}

#[inline]
fn build_update_ringbuffer_query_context(
	services: &Arc<Services>,
	target: &RingBufferTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
	identity: IdentityId,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let rb_ident = Fragment::internal(target.ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, target.ringbuffer.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::RingBuffer(resolved_rb)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	}
}

fn enforce_old_row_policies(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	shape: &RowShape,
	columns: &RecordBatch,
) -> Result<()> {
	if txn.identity().is_privileged() {
		return Ok(());
	}
	let ringbuffer = target.ringbuffer;
	let mut old_rows: Vec<(RowNumber, EncodedBytes)> = Vec::with_capacity(columns.num_rows());
	let sidecar_partitions = system_columns::partitions(columns)?;
	for (row_idx, &row_number) in row_numbers(columns)?.iter().enumerate() {
		let old_row_key = if sidecar_partitions.is_empty() {
			TaggedKey::from(RowKey::new(ringbuffer.id, row_number))
		} else {
			TaggedKey::from(PartitionedRowKey::new(ringbuffer.id, sidecar_partitions[row_idx], row_number))
		};
		let bytes = txn.get(&old_row_key)?.expect("bytes must exist for update").bytes;
		old_rows.push((row_number, bytes));
	}
	let old_columns = decode_rows_to_columns(shape, &old_rows)?;
	let old_columns = decode_returning_dictionaries(services, txn, &ringbuffer.columns, old_columns)?;
	PolicyEvaluator::new(services, symbols).enforce_write_policies(
		txn,
		target.namespace.name(),
		&ringbuffer.name,
		DataOp::Update,
		&old_columns,
		PolicyTargetType::RingBuffer,
	)
}

#[inline]
fn row_belongs_to_any_partition(partitions: &[PartitionedMetadata], row_number: RowNumber) -> bool {
	partitions
		.iter()
		.any(|p| !p.metadata.is_empty() && row_number.0 >= p.metadata.head && row_number.0 < p.metadata.tail)
}

#[inline]
fn update_ringbuffer_result(namespace: &str, ringbuffer: &str, updated: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
