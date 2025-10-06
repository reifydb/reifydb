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
			let mut row_numbers_to_delete = Vec::new();

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
					}),
				);

				let context = ExecutionContext {
					executor: self.clone(),
					source: None,
					batch_size: 1024,
					params: params.clone(),
				};

				// Initialize the operator before execution
				input_node.initialize(&mut std_txn, &context)?;

				while let Some(Batch {
					columns,
				}) = input_node.next(&mut std_txn)?
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

			// Delete the collected encoded numbers
			use crate::transaction::operation::RingBufferOperations;
			for row_number in row_numbers_to_delete {
				let row_key = RowKey {
					source: ring_buffer.id.into(),
					row: row_number,
				}
				.encode();

				// Remove the encoded if it exists
				if txn.contains_key(&row_key)? {
					txn.remove_from_ring_buffer(ring_buffer.clone(), row_number)?;
					deleted_count += 1;
				}
			}

			// Update metadata - we need to recalculate size
			// This is complex for ring buffers as we need to handle gaps
			// For simplicity, we'll just decrease the count
			if deleted_count > 0 && metadata.count > 0 {
				metadata.count = metadata.count.saturating_sub(deleted_count as u64);

				// If all entries are deleted, reset the buffer
				if metadata.count == 0 {
					metadata.head = 0;
					metadata.tail = 0;
				}
			}
		} else {
			// Delete all rows (clear the buffer)
			// Scan from head to tail and delete all entries
			use crate::transaction::operation::RingBufferOperations;
			if !metadata.is_empty() {
				let mut current = metadata.head;
				for _ in 0..metadata.count {
					let row_number = reifydb_type::RowNumber(current);
					let row_key = RowKey {
						source: ring_buffer.id.into(),
						row: row_number,
					}
					.encode();

					// Only delete if the encoded actually exists
					if txn.contains_key(&row_key)? {
						txn.remove_from_ring_buffer(ring_buffer.clone(), row_number)?;
						deleted_count += 1;
					}

					current = (current + 1) % metadata.capacity;
				}
			}

			// Reset metadata
			metadata.count = 0;
			metadata.head = 0;
			metadata.tail = 0;
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
