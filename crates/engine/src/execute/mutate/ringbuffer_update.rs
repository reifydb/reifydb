// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{Params, ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedRingBuffer},
	value::{column::Columns, encoded::EncodedValuesLayout},
};
use reifydb_rql::plan::physical::UpdateRingBufferNode;
use reifydb_type::{
	Fragment, Type, Value,
	diagnostic::{catalog::ringbuffer_not_found, engine},
	internal_error, return_error,
};

use super::coerce::coerce_value_to_column_type;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	encoding::encode_value,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
	transaction::operation::DictionaryOperations,
};

impl Executor {
	pub(crate) async fn update_ringbuffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: UpdateRingBufferNode,
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

		// Get current metadata - we need it to validate that rows exist
		let Some(metadata) = CatalogStore::find_ringbuffer_metadata(txn, ringbuffer.id).await? else {
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
		let resolved_source = Some(ResolvedPrimitive::RingBuffer(resolved_rb));

		// Create execution context
		let context = ExecutionContext {
			executor: self.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
			stack: Stack::new(),
		};

		let mut updated_count = 0;

		// Process all input batches - we need to handle compilation and
		// execution with proper transaction borrowing
		{
			let mut wrapped_txn = StandardTransaction::from(&mut *txn);
			let mut input_node = compile(*plan.input, &mut wrapped_txn, Arc::new(context.clone())).await;

			// Initialize the operator before execution
			input_node.initialize(&mut wrapped_txn, &context).await?;

			let mut mutable_context = context.clone();
			while let Some(Batch {
				columns,
			}) = input_node.next(&mut wrapped_txn, &mut mutable_context).await?
			{
				// Get encoded numbers from the Columns structure
				if columns.row_numbers.is_empty() {
					return_error!(engine::missing_row_number_column());
				}

				// Extract RowNumber data
				let row_numbers = &columns.row_numbers;

				let row_count = columns.row_count();

				use std::collections::HashMap;
				let mut column_map: HashMap<&str, usize> = HashMap::new();
				for (idx, col) in columns.iter().enumerate() {
					column_map.insert(col.name().text(), idx);
				}

				for row_idx in 0..row_count {
					let mut row = layout.allocate();

					// For each ring buffer column, find if it exists in the input columns
					for (rb_idx, rb_column) in ringbuffer.columns.iter().enumerate() {
						let mut value = if let Some(&input_idx) =
							column_map.get(rb_column.name.as_str())
						{
							columns[input_idx].data().get_value(row_idx)
						} else {
							Value::Undefined
						};

						// Create a ResolvedColumn for this ring buffer column
						let column_ident = Fragment::internal(&rb_column.name);
						let resolved_column = ResolvedColumn::new(
							column_ident,
							context.source.clone().unwrap(),
							rb_column.clone(),
						);

						value = coerce_value_to_column_type(
							value,
							rb_column.constraint.get_type(),
							resolved_column,
							&context,
						)?;

						// Validate the value against the column's constraint
						if let Err(e) = rb_column.constraint.validate(&value) {
							return Err(e);
						}

						// Dictionary encoding: if column has a dictionary binding, encode the
						// value
						let value = if let Some(dict_id) = rb_column.dictionary_id {
							let dictionary = CatalogStore::find_dictionary(
								wrapped_txn.command_mut(),
								dict_id,
							)
							.await?
							.ok_or_else(|| {
								internal_error!(
									"Dictionary {:?} not found for column {}",
									dict_id,
									rb_column.name
								)
							})?;
							let entry_id = wrapped_txn
								.command_mut()
								.insert_into_dictionary(&dictionary, &value)
								.await?;
							entry_id.to_value()
						} else {
							value
						};

						encode_value(&layout, &mut row, rb_idx, &value);
					}

					// Update the encoded using the existing RowNumber from the columns
					let row_number = row_numbers[row_idx];

					// Validate that the encoded number is within the valid range for this ring
					// buffer Ring buffer positions are from 0 to capacity-1
					if row_number.0 >= metadata.capacity {
						// Skip invalid encoded numbers silently or could return an error
						continue;
					}

					// Check if the encoded exists in the ring buffer
					// A encoded exists if it's within the current entries
					if metadata.is_empty() {
						// No entries, can't update
						continue;
					}

					// Calculate if this position is currently occupied
					let is_occupied = if !metadata.is_full() {
						// Not full: occupied positions are from 0 to current_size-1
						row_number.0 < metadata.count
					} else {
						// Full: all positions from 0 to capacity-1 are occupied
						true
					};

					if !is_occupied {
						// Position not occupied, skip
						continue;
					}

					// Update the encoded using interceptors
					use crate::transaction::operation::RingBufferOperations;
					wrapped_txn
						.command_mut()
						.update_ringbuffer(ringbuffer.clone(), row_number, row)
						.await?;

					updated_count += 1;
				}
			}
		}

		// Note: We don't update metadata because UPDATE doesn't change head, tail, or current_size
		// The ring buffer structure remains unchanged, only the data is modified

		// Return summary columns
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ringbuffer", Value::Utf8(ringbuffer.name)),
			("updated", Value::Uint8(updated_count as u64)),
		]))
	}
}
