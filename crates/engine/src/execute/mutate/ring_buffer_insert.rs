// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{Params, ResolvedColumn, ResolvedNamespace, ResolvedRingBuffer, ResolvedSource},
	return_error,
	value::{column::Columns, encoded::EncodedValuesLayout},
};
use reifydb_rql::plan::physical::InsertRingBufferNode;
use reifydb_type::{Fragment, IntoFragment, RowNumber, Type, Value, diagnostic::catalog::ring_buffer_not_found};

use super::coerce::coerce_value_to_column_type;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
};

impl Executor {
	pub(crate) fn insert_ring_buffer<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: InsertRingBufferNode<'a>,
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

		let ring_buffer_types: Vec<Type> =
			ring_buffer.columns.iter().map(|c| c.constraint.get_type()).collect();
		let layout = EncodedValuesLayout::new(&ring_buffer_types);

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::owned_internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::owned_internal(ring_buffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ring_buffer.clone());
		let resolved_source = Some(ResolvedSource::RingBuffer(resolved_rb));

		let execution_context = Arc::new(ExecutionContext {
			functions: self.functions.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone());

		let mut inserted_count = 0;

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context)?;

		// Process all input batches
		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn)?
		{
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

					// No auto-increment for ring buffers currently
					// TODO: Add support if needed

					// Create a ResolvedColumn for this ring buffer column
					let column_ident = Fragment::borrowed_internal(&rb_column.name);
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

					// Set the value in the encoded
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
						Value::IdentityId(v) => layout.set_identity_id(&mut row, rb_idx, v),
						Value::Uuid4(v) => layout.set_uuid4(&mut row, rb_idx, v),
						Value::Uuid7(v) => layout.set_uuid7(&mut row, rb_idx, v),
						Value::Blob(v) => layout.set_blob(&mut row, rb_idx, &v),
						Value::Int(v) => layout.set_int(&mut row, rb_idx, &v),
						Value::Uint(v) => layout.set_uint(&mut row, rb_idx, &v),
						Value::Decimal(v) => layout.set_decimal(&mut row, rb_idx, &v),
						Value::Undefined => layout.set_undefined(&mut row, rb_idx),
					}
				}

				// TODO: Check for primary key and handle upsert logic if needed
				// For now, just do simple circular buffer logic

				// Determine insert position
				let row_number = if metadata.is_empty() {
					// First insert
					RowNumber(1)
				} else if !metadata.is_full() {
					// Buffer not full, append at tail
					RowNumber(metadata.tail)
				} else {
					// Buffer full, overwrite at head
					RowNumber(metadata.head)
				};

				// Store the encoded using interceptors
				use crate::transaction::operation::RingBufferOperations;
				std_txn.command_mut().insert_into_ring_buffer_at(
					ring_buffer.clone(),
					row_number,
					row,
				)?;

				// Update metadata
				if metadata.is_empty() {
					metadata.count = 1;
					metadata.head = 1;
					metadata.tail = 2;
				} else if !metadata.is_full() {
					metadata.count += 1;
					// For 1-based indexing: next position after capacity is 1
					metadata.tail = if metadata.tail >= metadata.capacity {
						1
					} else {
						metadata.tail + 1
					};
				} else {
					// Buffer full, advance both head and tail
					metadata.head = if metadata.head >= metadata.capacity {
						1
					} else {
						metadata.head + 1
					};
					metadata.tail = if metadata.tail >= metadata.capacity {
						1
					} else {
						metadata.tail + 1
					};
				}

				inserted_count += 1;
			}
		}

		// Save updated metadata
		CatalogStore::update_ring_buffer_metadata(std_txn.command_mut(), metadata)?;

		// Return summary
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ring_buffer", Value::Utf8(ring_buffer.name)),
			("inserted", Value::Uint8(inserted_count as u64)),
		]))
	}
}
