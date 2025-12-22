// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, sequence::RowSequence};
use reifydb_core::{
	interface::{Params, ResolvedColumn, ResolvedNamespace, ResolvedRingBuffer, ResolvedSource},
	return_error,
	value::{column::Columns, encoded::EncodedValuesLayout},
};
use reifydb_rql::plan::physical::InsertRingBufferNode;
use reifydb_type::{Fragment, RowNumber, Type, Value, diagnostic::catalog::ringbuffer_not_found, internal_error};
use tracing::{debug_span, instrument};

use super::coerce::coerce_value_to_column_type;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	encoding::encode_value,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
	transaction::operation::DictionaryOperations,
};

impl Executor {
	#[instrument(name = "mutate::ringbuffer::insert", level = "trace", skip_all)]
	pub(crate) async fn insert_ringbuffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: InsertRingBufferNode,
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

		// Build storage layout types - use dictionary ID type for dictionary-encoded columns
		let mut ringbuffer_types: Vec<Type> = Vec::new();
		for c in &ringbuffer.columns {
			if let Some(dict_id) = c.dictionary_id {
				let dict_type = match CatalogStore::find_dictionary(txn, dict_id).await {
					Ok(Some(d)) => d.id_type,
					_ => c.constraint.get_type(),
				};
				ringbuffer_types.push(dict_type);
			} else {
				ringbuffer_types.push(c.constraint.get_type());
			}
		}
		let layout = EncodedValuesLayout::new(&ringbuffer_types);

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::internal(ringbuffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
		let resolved_source = Some(ResolvedSource::RingBuffer(resolved_rb));

		let execution_context = Arc::new(ExecutionContext {
			executor: self.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
			stack: Stack::new(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone()).await;

		let mut inserted_count = 0;

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context).await?;

		// Process all input batches
		let mut mutable_context = (*execution_context).clone();
		let _batch_loop_span = debug_span!("insert_batch_loop").entered();
		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn, &mut mutable_context).await?
		{
			let row_count = columns.row_count();
			let _rows_span = debug_span!("process_rows", row_count).entered();

			for row_idx in 0..row_count {
				let mut row = layout.allocate();

				// For each ring buffer column, find if it exists in the input columns
				for (rb_idx, rb_column) in ringbuffer.columns.iter().enumerate() {
					let mut value = if let Some(input_column) =
						columns.iter().find(|col| col.name() == rb_column.name)
					{
						input_column.data().get_value(row_idx)
					} else {
						Value::Undefined
					};

					// No auto-increment for ring buffers currently
					// TODO: Add support if needed

					// Create a ResolvedColumn for this ring buffer column
					let column_ident = Fragment::internal(&rb_column.name);
					let resolved_column = ResolvedColumn::new(
						column_ident,
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
					if let Err(e) = rb_column.constraint.validate(&value) {
						return Err(e);
					}

					// Dictionary encoding: if column has a dictionary binding, encode the value
					let value = if let Some(dict_id) = rb_column.dictionary_id {
						let _dict_span = debug_span!("dictionary_encode").entered();
						let dictionary =
							CatalogStore::find_dictionary(std_txn.command_mut(), dict_id)
								.await?
								.ok_or_else(|| {
									internal_error!(
										"Dictionary {:?} not found for column {}",
										dict_id,
										rb_column.name
									)
								})?;
						let entry_id = std_txn
							.command_mut()
							.insert_into_dictionary(&dictionary, &value)
							.await?;
						entry_id.to_value()
					} else {
						value
					};

					encode_value(&layout, &mut row, rb_idx, &value);
				}

				// TODO: Check for primary key and handle upsert logic if needed

				use crate::transaction::operation::RingBufferOperations;

				// If buffer is full, delete the oldest entry first
				if metadata.is_full() {
					let oldest_row = RowNumber(metadata.head);
					std_txn.command_mut()
						.remove_from_ringbuffer(ringbuffer.clone(), oldest_row)
						.await?;
					// Advance head to next oldest item
					metadata.head += 1;
					metadata.count -= 1;
				}

				// Get next row number from sequence (monotonically increasing)
				let row_number = {
					let _seq_span = debug_span!("allocate_row_number").entered();
					RowSequence::next_row_number_for_ringbuffer(
						std_txn.command_mut(),
						ringbuffer.id,
					)
					.await?
				};

				// Store the row
				{
					let _store_span = debug_span!("store_row").entered();
					std_txn.command_mut()
						.insert_ringbuffer_at(ringbuffer.clone(), row_number, row)
						.await?;
				}

				// Update metadata
				if metadata.is_empty() {
					metadata.head = row_number.0;
				}
				metadata.count += 1;
				metadata.tail = row_number.0 + 1; // Next insert position

				inserted_count += 1;
			}
		}

		// Save updated metadata
		CatalogStore::update_ringbuffer_metadata(std_txn.command_mut(), metadata).await?;

		// Return summary
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ringbuffer", Value::Utf8(ringbuffer.name)),
			("inserted", Value::Uint8(inserted_count as u64)),
		]))
	}
}
