// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{Params, ResolvedNamespace, ResolvedPrimitive, ResolvedRingBuffer, RowKey},
	value::column::Columns,
};
use reifydb_rql::plan::physical::DeleteRingBufferNode;
use reifydb_type::{
	Fragment, Value,
	diagnostic::{catalog::ringbuffer_not_found, engine},
	return_error,
};

use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
};

impl Executor {
	pub(crate) async fn delete_ringbuffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: DeleteRingBufferNode,
		params: Params,
	) -> crate::Result<Columns> {
		let namespace_name = plan.target.namespace().name();
		let namespace = CatalogStore::find_namespace_by_name(txn, namespace_name).await?.unwrap();

		let ringbuffer_name = plan.target.name();
		let Some(ringbuffer) =
			CatalogStore::find_ringbuffer_by_name(txn, namespace.id, ringbuffer_name).await?
		else {
			let fragment = Fragment::internal(plan.target.name());
			return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
		};

		// Get current metadata
		let Some(mut metadata) = CatalogStore::find_ringbuffer_metadata(txn, ringbuffer.id).await? else {
			let fragment = Fragment::internal(plan.target.name());
			return_error!(ringbuffer_not_found(fragment, namespace_name, ringbuffer_name));
		};

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::internal(ringbuffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
		let resolved_source = Some(ResolvedPrimitive::RingBuffer(resolved_rb));

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
				)
				.await;

				let context = ExecutionContext {
					executor: self.clone(),
					source: None,
					batch_size: 1024,
					params: params.clone(),
					stack: Stack::new(),
				};

				// Initialize the operator before execution
				input_node.initialize(&mut std_txn, &context).await?;

				let mut mutable_context = context.clone();
				while let Some(Batch {
					columns,
				}) = input_node.next(&mut std_txn, &mut mutable_context).await?
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
				let key = RowKey::encoded(ringbuffer.id, row_num);

				if txn.contains_key(&key).await? {
					if row_numbers_to_delete.contains(&row_num) {
						// Delete this row
						txn.remove_from_ringbuffer(ringbuffer.clone(), row_num).await?;
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
				let row_key = RowKey::encoded(ringbuffer.id, row_number);

				// Only delete if the entry exists
				if txn.contains_key(&row_key).await? {
					txn.remove_from_ringbuffer(ringbuffer.clone(), row_number).await?;
					deleted_count += 1;
				}
			}

			// Reset metadata - empty buffer: head = tail
			metadata.count = 0;
			metadata.head = metadata.tail;
		}

		// Save updated metadata
		CatalogStore::update_ringbuffer_metadata(txn, metadata).await?;

		// Return summary
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ringbuffer", Value::Utf8(ringbuffer.name)),
			("deleted", Value::Uint8(deleted_count as u64)),
		]))
	}
}
