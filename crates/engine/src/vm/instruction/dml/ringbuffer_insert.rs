// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::catalog::{namespace_not_found, ringbuffer_not_found},
	interface::{
		catalog::{
			policy::{DataOp, PolicyTargetType},
			ringbuffer::RingBufferMetadata,
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
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let ringbuffer_name = plan.target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id(), ringbuffer_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};

	// Get or create shape with proper field names and constraints
	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;

	// Create resolved source for the ring buffer
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	let resolved_source = Some(ResolvedShape::RingBuffer(resolved_rb));

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	let mut inserted_count = 0;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();

	// Resolve partition column indices once (empty vec for global)
	let partition_col_indices: Vec<usize> = ringbuffer
		.partition_by
		.iter()
		.map(|pb_col| ringbuffer.columns.iter().position(|c| c.name == *pb_col).unwrap())
		.collect();

	// Cache metadata for all partitions encountered
	let mut partition_metadata_cache: HashMap<Vec<Value>, RingBufferMetadata> = HashMap::new();

	// Initialize the operator before execution
	input_node.initialize(txn, &execution_context)?;

	// Process all input batches
	let mut mutable_context = (*execution_context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		// Enforce write policies before processing rows
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace_name,
			ringbuffer_name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::RingBuffer,
		)?;

		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			let mut row = shape.allocate();
			let mut row_values: Vec<Value> = Vec::with_capacity(ringbuffer.columns.len());

			// For each ring buffer column, find if it exists in the input columns
			for (rb_idx, rb_column) in ringbuffer.columns.iter().enumerate() {
				let mut value = if let Some(input_column) =
					columns.iter().find(|col| col.name() == rb_column.name)
				{
					input_column.data().get_value(row_idx)
				} else {
					Value::none()
				};

				// Create a ResolvedColumn for this ring buffer column
				let column_ident = columns
					.iter()
					.find(|col| col.name() == rb_column.name)
					.map(|col| col.name().clone())
					.unwrap_or_else(|| Fragment::internal(&rb_column.name));
				let resolved_column = ResolvedColumn::new(
					column_ident.clone(),
					execution_context.source.clone().unwrap(),
					rb_column.clone(),
				);

				value = coerce_value_to_column_type(
					value,
					rb_column.constraint.get_type(),
					resolved_column,
					&execution_context,
				)?;

				// Validate the value against the column's constraint
				if let Err(mut e) = rb_column.constraint.validate(&value) {
					e.0.fragment = column_ident.clone();
					return Err(e);
				}

				// Dictionary encoding: if column has a dictionary binding, encode the value
				let value = if let Some(dict_id) = rb_column.dictionary_id {
					let dictionary =
						services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
							internal_error!(
								"Dictionary {:?} not found for column {}",
								dict_id,
								rb_column.name
							)
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

			let partition_key: Vec<Value> =
				partition_col_indices.iter().map(|&idx| row_values[idx].clone()).collect();

			// Get or create partition metadata
			if !partition_metadata_cache.contains_key(&partition_key) {
				let existing =
					services.catalog.find_partition_metadata(txn, &ringbuffer, &partition_key)?;
				let m = existing
					.unwrap_or_else(|| RingBufferMetadata::new(ringbuffer.id, ringbuffer.capacity));
				partition_metadata_cache.insert(partition_key.clone(), m);
			}
			let current_metadata = partition_metadata_cache.get_mut(&partition_key).unwrap();

			// If buffer is full, delete the oldest entry for THIS partition
			if current_metadata.is_full() {
				// Find the actual oldest row belonging to this partition
				let mut evict_pos = current_metadata.head;
				loop {
					let key = RowKey::encoded(ringbuffer.id, RowNumber(evict_pos));
					if let Some(row_data) = txn.get(&key)?
						&& (partition_col_indices.is_empty()
							|| row_matches_partition(
								&shape,
								&row_data.row,
								&partition_col_indices,
								&partition_key,
							)) {
						txn.remove_from_ringbuffer(&ringbuffer, RowNumber(evict_pos))?;
						break;
					}
					evict_pos += 1;
					if evict_pos >= current_metadata.tail {
						break;
					}
				}
				// Advance head to next row belonging to this partition
				current_metadata.head = evict_pos + 1;
				while current_metadata.head < current_metadata.tail {
					let key = RowKey::encoded(ringbuffer.id, RowNumber(current_metadata.head));
					if let Some(row_data) = txn.get(&key)?
						&& (partition_col_indices.is_empty()
							|| row_matches_partition(
								&shape,
								&row_data.row,
								&partition_col_indices,
								&partition_key,
							)) {
						break;
					}
					current_metadata.head += 1;
				}
				current_metadata.count -= 1;
			}

			// Get next row number from sequence (monotonically increasing)
			let row_number = services.catalog.next_row_number_for_ringbuffer(txn, ringbuffer.id)?;

			// Store the row
			let stored_row = txn.insert_ringbuffer_at(&ringbuffer, &shape, row_number, row.clone())?;
			if plan.returning.is_some() {
				returned_rows.push((row_number, stored_row));
			}

			// Update metadata
			if current_metadata.is_empty() {
				current_metadata.head = row_number.0;
			}
			current_metadata.count += 1;
			current_metadata.tail = row_number.0 + 1; // Next insert position

			inserted_count += 1;
		}
	}

	// Save all modified partition metadata via unified API
	for (partition_key, m) in &partition_metadata_cache {
		services.catalog.save_partition_metadata(txn, &ringbuffer, partition_key, m)?;
	}

	// If RETURNING clause is present, evaluate expressions against inserted rows
	if let Some(returning_exprs) = &plan.returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.name)),
		("inserted", Value::Uint8(inserted_count as u64)),
	]))
}

fn row_matches_partition(
	shape: &RowShape,
	row: &EncodedRow,
	partition_col_indices: &[usize],
	expected_values: &[Value],
) -> bool {
	partition_col_indices.iter().zip(expected_values).all(|(&idx, expected)| shape.get_value(row, idx) == *expected)
}
