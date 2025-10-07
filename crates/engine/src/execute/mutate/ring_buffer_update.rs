// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{Params, ResolvedColumn, ResolvedNamespace, ResolvedRingBuffer, ResolvedSource},
	value::{column::Columns, encoded::EncodedValuesLayout},
};
use reifydb_rql::plan::physical::UpdateRingBufferNode;
use reifydb_type::{
	Fragment, IntoFragment, Type, Value,
	diagnostic::{catalog::ring_buffer_not_found, engine},
	return_error,
};

use super::coerce::coerce_value_to_column_type;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
};

impl Executor {
	pub(crate) fn update_ring_buffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: UpdateRingBufferNode<'a>,
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

		// Get current metadata - we need it to validate that rows exist
		let Some(metadata) = CatalogStore::find_ring_buffer_metadata(txn, ring_buffer.id)? else {
			let fragment = plan.target.name().into_fragment();
			return_error!(ring_buffer_not_found(fragment, namespace_name, ring_buffer_name));
		};

		let ring_buffer_types: Vec<Type> =
			ring_buffer.columns.iter().map(|c| c.constraint.get_type()).collect();
		let layout = EncodedValuesLayout::new(&ring_buffer_types);

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::owned_internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::owned_internal(ring_buffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ring_buffer.clone());
		let resolved_source = Some(ResolvedSource::RingBuffer(resolved_rb));

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
			let mut input_node = compile(*plan.input, &mut wrapped_txn, Arc::new(context.clone()));

			// Initialize the operator before execution
			input_node.initialize(&mut wrapped_txn, &context)?;

			let mut mutable_context = context.clone();
			while let Some(Batch {
				columns,
			}) = input_node.next(&mut wrapped_txn, &mut mutable_context)?
			{
				// Get encoded numbers from the Columns structure
				if columns.row_numbers.is_empty() {
					return_error!(engine::missing_row_number_column());
				}

				// Extract RowNumber data
				let row_numbers = &columns.row_numbers;

				let row_count = columns.row_count();

				for row_idx in 0..row_count {
					let mut row = layout.allocate();

					// For each ring buffer column, find if it exists in the input columns
					for (rb_idx, rb_column) in ring_buffer.columns.iter().enumerate() {
						let mut value = if let Some(input_column) =
							columns.iter().find(|col| col.name() == rb_column.name)
						{
							input_column.data().get_value(row_idx)
						} else {
							Value::Undefined
						};

						// Create a ResolvedColumn for this ring buffer column
						let column_ident = Fragment::borrowed_internal(&rb_column.name);
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

						match value {
							Value::Boolean(v) => layout.set_bool(&mut row, rb_idx, v),
							Value::Float4(v) => layout.set_f32(&mut row, rb_idx, *v),
							Value::Float8(v) => layout.set_f64(&mut row, rb_idx, *v),
							Value::Int1(v) => layout.set_i8(&mut row, rb_idx, v),
							Value::Int2(v) => layout.set_i16(&mut row, rb_idx, v),
							Value::Int4(v) => layout.set_i32(&mut row, rb_idx, v),
							Value::Int8(v) => layout.set_i64(&mut row, rb_idx, v),
							Value::Int16(v) => layout.set_i128(&mut row, rb_idx, v),
							Value::Utf8(v) => layout.set_utf8(&mut row, rb_idx, v),
							Value::Uint1(v) => layout.set_u8(&mut row, rb_idx, v),
							Value::Uint2(v) => layout.set_u16(&mut row, rb_idx, v),
							Value::Uint4(v) => layout.set_u32(&mut row, rb_idx, v),
							Value::Uint8(v) => layout.set_u64(&mut row, rb_idx, v),
							Value::Uint16(v) => layout.set_u128(&mut row, rb_idx, v),
							Value::Date(v) => layout.set_date(&mut row, rb_idx, v),
							Value::DateTime(v) => layout.set_datetime(&mut row, rb_idx, v),
							Value::Time(v) => layout.set_time(&mut row, rb_idx, v),
							Value::Interval(v) => layout.set_interval(&mut row, rb_idx, v),
							Value::RowNumber(_v) => {}
							Value::IdentityId(v) => {
								layout.set_identity_id(&mut row, rb_idx, v)
							}
							Value::Uuid4(v) => layout.set_uuid4(&mut row, rb_idx, v),
							Value::Uuid7(v) => layout.set_uuid7(&mut row, rb_idx, v),
							Value::Blob(v) => layout.set_blob(&mut row, rb_idx, &v),
							Value::Int(v) => layout.set_int(&mut row, rb_idx, &v),
							Value::Uint(v) => layout.set_uint(&mut row, rb_idx, &v),
							Value::Decimal(v) => layout.set_decimal(&mut row, rb_idx, &v),
							Value::Undefined => layout.set_undefined(&mut row, rb_idx),
						}
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
					wrapped_txn.command_mut().update_ring_buffer(
						ring_buffer.clone(),
						row_number,
						row,
					)?;

					updated_count += 1;
				}
			}
		}

		// Note: We don't update metadata because UPDATE doesn't change head, tail, or current_size
		// The ring buffer structure remains unchanged, only the data is modified

		// Return summary columns
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ring_buffer", Value::Utf8(ring_buffer.name)),
			("updated", Value::Uint8(updated_count as u64)),
		]))
	}
}
