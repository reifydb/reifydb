// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::catalog::{namespace_not_found, ringbuffer_not_found},
	interface::{
		catalog::{
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			ringbuffer::{RingBuffer, RingBufferMetadata},
		},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedRingBuffer, ResolvedShape},
	},
	internal_error,
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::InsertRingBufferNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::{
	coerce::coerce_value_to_column_type,
	context::RingBufferTarget,
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::{dictionary::DictionaryOperations, ringbuffer::RingBufferOperations},
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
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
) -> Result<Columns> {
	let InsertRingBufferNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, ringbuffer) = resolve_insert_ringbuffer_target(services, txn, &target)?;
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;
	let target_data = RingBufferTarget {
		namespace: &namespace,
		ringbuffer: &ringbuffer,
	};
	let context = build_insert_ringbuffer_query_context(services, &target_data, &params, symbols);
	let mut input_node = compile(*input, txn, context.clone());
	input_node.initialize(txn, &context)?;

	let partition_col_indices = compute_partition_col_indices(&ringbuffer);
	let mut partition_metadata_cache: HashMap<Vec<Value>, RingBufferMetadata> = HashMap::new();
	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let has_returning = returning.is_some();

	let mut mutable_context = (*context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&ringbuffer.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;

		let row_count = columns.row_count();
		for row_idx in 0..row_count {
			let (row, row_values) = build_insert_ringbuffer_row(
				services,
				txn,
				&target_data,
				&shape,
				&columns,
				&context,
				row_idx,
			)?;
			let partition_key: Vec<Value> =
				partition_col_indices.iter().map(|&idx| row_values[idx].clone()).collect();
			ensure_partition_metadata(
				services,
				txn,
				&target_data,
				&partition_key,
				&mut partition_metadata_cache,
			)?;
			let current_metadata = partition_metadata_cache.get_mut(&partition_key).unwrap();

			if current_metadata.is_full() {
				evict_oldest_for_partition(
					txn,
					&target_data,
					&shape,
					&partition_col_indices,
					&partition_key,
					current_metadata,
				)?;
			}

			let row_number = services.catalog.next_row_number_for_ringbuffer(txn, ringbuffer.id)?;
			let stored_row = txn.insert_ringbuffer_at(&ringbuffer, &shape, row_number, row)?;
			if has_returning {
				returned_rows.push((row_number, stored_row));
			}
			update_metadata_after_insert(current_metadata, row_number);
			inserted_count += 1;
		}
	}

	save_all_partition_metadata(services, txn, &ringbuffer, &partition_metadata_cache)?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(insert_ringbuffer_result(namespace.name(), &ringbuffer.name, inserted_count))
}

#[inline]
fn resolve_insert_ringbuffer_target(
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
fn build_insert_ringbuffer_query_context(
	services: &Arc<Services>,
	target: &RingBufferTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let rb_ident = Fragment::internal(target.ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, target.ringbuffer.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::RingBuffer(resolved_rb)),
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	})
}

#[inline]
fn compute_partition_col_indices(ringbuffer: &RingBuffer) -> Vec<usize> {
	ringbuffer
		.partition_by
		.iter()
		.map(|pb_col| ringbuffer.columns.iter().position(|c| c.name == *pb_col).unwrap())
		.collect()
}

fn build_insert_ringbuffer_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	shape: &RowShape,
	columns: &Columns,
	context: &Arc<QueryContext>,
	row_idx: usize,
) -> Result<(EncodedRow, Vec<Value>)> {
	let mut row = shape.allocate();
	let mut row_values: Vec<Value> = Vec::with_capacity(target.ringbuffer.columns.len());

	for (rb_idx, rb_column) in target.ringbuffer.columns.iter().enumerate() {
		let mut value = if let Some(input_column) = columns.iter().find(|col| col.name() == rb_column.name) {
			input_column.data().get_value(row_idx)
		} else {
			Value::none()
		};

		let column_ident = columns
			.iter()
			.find(|col| col.name() == rb_column.name)
			.map(|col| col.name().clone())
			.unwrap_or_else(|| Fragment::internal(&rb_column.name));
		let resolved_column =
			ResolvedColumn::new(column_ident.clone(), context.source.clone().unwrap(), rb_column.clone());

		value = coerce_value_to_column_type(value, rb_column.constraint.get_type(), resolved_column, context)?;
		if let Err(mut e) = rb_column.constraint.validate(&value) {
			e.0.fragment = column_ident.clone();
			return Err(e);
		}

		let value = if let Some(dict_id) = rb_column.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, rb_column.name)
			})?;
			let entry_id = txn.insert_into_dictionary(&dictionary, &value)?;
			entry_id.to_value()
		} else {
			value
		};

		row_values.push(value.clone());
		shape.set_value(&mut row, rb_idx, &value);
	}

	let now_nanos = services.runtime_context.clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	Ok((row, row_values))
}

#[inline]
fn ensure_partition_metadata(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	partition_key: &[Value],
	cache: &mut HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	if !cache.contains_key(partition_key) {
		let existing = services.catalog.find_partition_metadata(txn, target.ringbuffer, partition_key)?;
		let m = existing
			.unwrap_or_else(|| RingBufferMetadata::new(target.ringbuffer.id, target.ringbuffer.capacity));
		cache.insert(partition_key.to_vec(), m);
	}
	Ok(())
}

fn evict_oldest_for_partition(
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	shape: &RowShape,
	partition_col_indices: &[usize],
	partition_key: &[Value],
	metadata: &mut RingBufferMetadata,
) -> Result<()> {
	let ringbuffer = target.ringbuffer;
	let mut evict_pos = metadata.head;
	loop {
		let key = RowKey::encoded(ringbuffer.id, RowNumber(evict_pos));
		if let Some(row_data) = txn.get(&key)?
			&& (partition_col_indices.is_empty()
				|| row_matches_partition(shape, &row_data.row, partition_col_indices, partition_key))
		{
			txn.remove_from_ringbuffer(ringbuffer, RowNumber(evict_pos))?;
			break;
		}
		evict_pos += 1;
		if evict_pos >= metadata.tail {
			break;
		}
	}
	metadata.head = evict_pos + 1;
	while metadata.head < metadata.tail {
		let key = RowKey::encoded(ringbuffer.id, RowNumber(metadata.head));
		if let Some(row_data) = txn.get(&key)?
			&& (partition_col_indices.is_empty()
				|| row_matches_partition(shape, &row_data.row, partition_col_indices, partition_key))
		{
			break;
		}
		metadata.head += 1;
	}
	metadata.count -= 1;
	Ok(())
}

#[inline]
fn update_metadata_after_insert(metadata: &mut RingBufferMetadata, row_number: RowNumber) {
	if metadata.is_empty() {
		metadata.head = row_number.0;
	}
	metadata.count += 1;
	metadata.tail = row_number.0 + 1;
}

#[inline]
fn save_all_partition_metadata(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	cache: &HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	for (partition_key, m) in cache {
		services.catalog.save_partition_metadata(txn, ringbuffer, partition_key, m)?;
	}
	Ok(())
}

#[inline]
fn insert_ringbuffer_result(namespace: &str, ringbuffer: &str, inserted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}

fn row_matches_partition(
	shape: &RowShape,
	row: &EncodedRow,
	partition_col_indices: &[usize],
	expected_values: &[Value],
) -> bool {
	partition_col_indices.iter().zip(expected_values).all(|(&idx, expected)| shape.get_value(row, idx) == *expected)
}
