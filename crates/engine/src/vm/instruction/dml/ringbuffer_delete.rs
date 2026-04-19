// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
	},
	interface::{
		catalog::policy::{DataOp, PolicyTargetType},
		resolved::{ResolvedNamespace, ResolvedRingBuffer, ResolvedShape},
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DeleteRingBufferNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use super::{
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_ringbuffer_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::ringbuffer::RingBufferOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

pub(crate) fn delete_ringbuffer(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteRingBufferNode,
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

	// Create resolved source for the ring buffer
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	let resolved_source = Some(ResolvedShape::RingBuffer(resolved_rb));

	// Resolve partition column indices once (empty vec for global)
	let partition_col_indices: Vec<usize> = ringbuffer
		.partition_by
		.iter()
		.map(|pb_col| ringbuffer.columns.iter().position(|c| c.name == *pb_col).unwrap())
		.collect();

	let shape = get_or_create_ringbuffer_shape(&services.catalog, &ringbuffer, txn)?;
	let mut deleted_count = 0;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();

	if let Some(input_plan) = plan.input {
		// Filtered delete: collect row numbers to delete from the filter
		let mut row_numbers_to_delete = collections::HashSet::new();

		{
			let mut input_node = compile(
				*input_plan,
				txn,
				Arc::new(QueryContext {
					services: services.clone(),
					source: resolved_source.clone(),
					batch_size: 1024,
					params: params.clone(),
					symbols: symbols.clone(),
					identity: IdentityId::root(),
				}),
			);

			let context = QueryContext {
				services: services.clone(),
				source: None,
				batch_size: 1024,
				params: params.clone(),
				symbols: symbols.clone(),
				identity: IdentityId::root(),
			};

			input_node.initialize(txn, &context)?;

			let mut mutable_context = context.clone();
			while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
				PolicyEvaluator::new(services, symbols).enforce_write_policies(
					txn,
					namespace.name(),
					&ringbuffer.name,
					DataOp::Delete,
					&columns,
					PolicyTargetType::RingBuffer,
				)?;

				if columns.row_numbers.is_empty() {
					return_error!(engine::missing_row_number_column());
				}

				row_numbers_to_delete.extend(columns.row_numbers.iter().copied());
			}
		}

		// Load all partitions and process each
		let partitions = services.catalog.list_ringbuffer_partitions(txn, &ringbuffer)?;

		for partition_info in partitions {
			let partition_key = partition_info.partition_values.clone();
			let mut partition = partition_info.metadata;
			let mut min_remaining_row: Option<u64> = None;
			let mut partition_deleted = 0u64;

			for row_num_value in partition.head..partition.tail {
				let row_num = RowNumber(row_num_value);
				let key = RowKey::encoded(ringbuffer.id, row_num);

				if let Some(row_data) = txn.get(&key)? {
					// Skip rows that don't belong to this partition
					if !partition_col_indices.is_empty()
						&& !row_matches_partition(
							&shape,
							&row_data.row,
							&partition_col_indices,
							&partition_key,
						) {
						continue;
					}

					if row_numbers_to_delete.contains(&row_num) {
						let deleted_values =
							txn.remove_from_ringbuffer(&ringbuffer, row_num)?;
						if plan.returning.is_some() {
							returned_rows.push((row_num, deleted_values));
						}
						partition_deleted += 1;
						deleted_count += 1;
					} else {
						min_remaining_row = Some(min_remaining_row
							.map_or(row_num_value, |m: u64| m.min(row_num_value)));
					}
				}
			}

			if partition_deleted > 0 {
				let remaining_count = partition.count.saturating_sub(partition_deleted);
				if remaining_count == 0 {
					partition.count = 0;
					partition.head = partition.tail;
				} else {
					partition.count = remaining_count;
					partition.head = min_remaining_row.unwrap();
				}

				services.catalog.save_partition_metadata(
					txn,
					&ringbuffer,
					&partition_key,
					&partition,
				)?;
			}
		}
	} else {
		// Delete all entries across all partitions
		let partitions = services.catalog.list_ringbuffer_partitions(txn, &ringbuffer)?;

		for partition_info in partitions {
			let partition_key = partition_info.partition_values.clone();
			let mut partition = partition_info.metadata;

			for row_num_value in partition.head..partition.tail {
				let row_number = RowNumber(row_num_value);
				let row_key = RowKey::encoded(ringbuffer.id, row_number);

				if let Some(row_data) = txn.get(&row_key)? {
					// Skip rows that don't belong to this partition
					if !partition_col_indices.is_empty()
						&& !row_matches_partition(
							&shape,
							&row_data.row,
							&partition_col_indices,
							&partition_key,
						) {
						continue;
					}

					let deleted_values = txn.remove_from_ringbuffer(&ringbuffer, row_number)?;
					if plan.returning.is_some() {
						returned_rows.push((row_number, deleted_values));
					}
					deleted_count += 1;
				}
			}

			// Reset metadata — empty buffer: head = tail
			partition.count = 0;
			partition.head = partition.tail;
			services.catalog.save_partition_metadata(txn, &ringbuffer, &partition_key, &partition)?;
		}
	}

	// If RETURNING clause is present, evaluate expressions against deleted rows
	if let Some(returning_exprs) = &plan.returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("ringbuffer", Value::Utf8(ringbuffer.name)),
		("deleted", Value::Uint8(deleted_count as u64)),
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
