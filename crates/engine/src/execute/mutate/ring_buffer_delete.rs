// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{
		EncodableKey, MultiVersionQueryTransaction, Params, ResolvedNamespace, ResolvedRingBuffer,
		ResolvedSource, RowKey,
	},
	value::column::Columns,
};
use reifydb_rql::plan::physical::DeleteRingBufferNode;
use reifydb_type::{
	Fragment, IntoFragment, Value,
	diagnostic::{catalog::ring_buffer_not_found, engine},
	return_error,
};

use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
};

impl Executor {
	pub(crate) fn delete_ring_buffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: DeleteRingBufferNode<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		let namespace_name = plan.target.namespace().name();
		let namespace = CatalogStore::find_namespace_by_name(txn, namespace_name)?.unwrap();

		let ring_buffer_name = plan.target.name();
		let Some(ring_buffer) = CatalogStore::find_ring_buffer_by_name(txn, namespace.id, ring_buffer_name)?
		else {
			let fragment = plan.target.name().into_fragment();
			return_error!(ring_buffer_not_found(fragment.clone(), namespace_name, ring_buffer_name));
		};

		// Get current metadata
		let Some(mut metadata) = CatalogStore::find_ring_buffer_metadata(txn, ring_buffer.id)? else {
			let fragment = plan.target.name().into_fragment();
			return_error!(ring_buffer_not_found(fragment, namespace_name, ring_buffer_name));
		};

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::owned_internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::owned_internal(ring_buffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ring_buffer.clone());
		let resolved_source = Some(ResolvedSource::RingBuffer(resolved_rb));

		let mut deleted_count = 0;

		if let Some(input_plan) = plan.input {
			// Delete specific rows based on input plan
			// Collect row numbers to delete from the filter
			let mut row_numbers_to_delete = std::collections::HashSet::new();

			{
				let mut std_txn = StandardTransaction::from(&mut *txn);
				let mut input_node = compile(
					*input_plan,
					&mut std_txn,
					Arc::new(ExecutionContext {
						executor: self.clone(),
						source: resolved_source.clone(),
						batch_size: 1024,
						params: params.clone(),
						stack: Stack::new(),
					}),
				);

				let context = ExecutionContext {
					executor: self.clone(),
					source: None,
					batch_size: 1024,
					params: params.clone(),
					stack: Stack::new(),
				};

				// Initialize the operator before execution
				input_node.initialize(&mut std_txn, &context)?;

				let mut mutable_context = context.clone();
				while let Some(Batch {
					columns,
				}) = input_node.next(&mut std_txn, &mut mutable_context)?
				{
					// Get encoded numbers from the Columns structure
					if columns.row_numbers.is_empty() {
						return_error!(engine::missing_row_number_column());
					}

					// Extract RowNumber data
					let row_numbers = &columns.row_numbers;

					row_numbers_to_delete.extend(row_numbers.iter().copied());
				}
			}

			// With monotonically increasing row numbers, we only delete the specified rows
			// and update head to the minimum remaining row number
			use crate::transaction::operation::RingBufferOperations;

			// Delete the specified rows and track remaining items
			let mut min_remaining_row: Option<u64> = None;

			// Iterate from head to tail-1 (the range of row numbers in the buffer)
			for row_num_value in metadata.head..metadata.tail {
				let row_num = reifydb_type::RowNumber(row_num_value);
				let key = RowKey {
					source: ring_buffer.id.into(),
					row: row_num,
				};

				if txn.contains_key(&key.encode())? {
					if row_numbers_to_delete.contains(&row_num) {
						// Delete this row
						txn.remove_from_ring_buffer(ring_buffer.clone(), row_num)?;
						deleted_count += 1;
					} else {
						// Track minimum remaining row number
						min_remaining_row = Some(min_remaining_row
							.map_or(row_num_value, |m| m.min(row_num_value)));
					}
				}
			}

			// Update metadata
			let remaining_count = metadata.count.saturating_sub(deleted_count as u64);
			if remaining_count == 0 {
				metadata.count = 0;
				// Empty buffer: set head = tail (RowSequence will provide next row number)
				metadata.head = metadata.tail;
			} else {
				metadata.count = remaining_count;
				metadata.head = min_remaining_row.unwrap();
				// tail stays the same - next row number comes from RowSequence
			}
		} else {
			// Delete all rows (clear the buffer)
			use crate::transaction::operation::RingBufferOperations;

			// Delete all entries in the row number range
			for row_num_value in metadata.head..metadata.tail {
				let row_number = reifydb_type::RowNumber(row_num_value);
				let row_key = RowKey {
					source: ring_buffer.id.into(),
					row: row_number,
				}
				.encode();

				// Only delete if the entry exists
				if txn.contains_key(&row_key)? {
					txn.remove_from_ring_buffer(ring_buffer.clone(), row_number)?;
					deleted_count += 1;
				}
			}

			// Reset metadata - empty buffer: head = tail
			metadata.count = 0;
			metadata.head = metadata.tail;
		}

		// Save updated metadata
		CatalogStore::update_ring_buffer_metadata(txn, metadata)?;

		// Return summary
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ring_buffer", Value::Utf8(ring_buffer.name)),
			("deleted", Value::Uint8(deleted_count as u64)),
		]))
	}
}
