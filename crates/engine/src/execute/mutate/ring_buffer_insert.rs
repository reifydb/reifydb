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
use reifydb_type::{
	Fragment, IntoFragment, RowNumber, Type, Value, diagnostic::catalog::ring_buffer_not_found, internal_error,
};
use tracing::{debug_span, instrument};

use super::coerce::coerce_value_to_column_type;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
	transaction::operation::DictionaryOperations,
};

impl Executor {
	#[instrument(name = "insert_ring_buffer", level = "trace", skip_all)]
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

		// Build storage layout types - use dictionary ID type for dictionary-encoded columns
		let ring_buffer_types: Vec<Type> = ring_buffer
			.columns
			.iter()
			.map(|c| {
				if let Some(dict_id) = c.dictionary_id {
					CatalogStore::find_dictionary(txn, dict_id)
						.ok()
						.flatten()
						.map(|d| d.id_type)
						.unwrap_or_else(|| c.constraint.get_type())
				} else {
					c.constraint.get_type()
				}
			})
			.collect();
		let layout = EncodedValuesLayout::new(&ring_buffer_types);

		// Create resolved source for the ring buffer
		let namespace_ident = Fragment::owned_internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let rb_ident = Fragment::owned_internal(ring_buffer.name.clone());
		let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ring_buffer.clone());
		let resolved_source = Some(ResolvedSource::RingBuffer(resolved_rb));

		let execution_context = Arc::new(ExecutionContext {
			executor: self.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
			stack: Stack::new(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone());

		let mut inserted_count = 0;

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context)?;

		// Process all input batches
		let mut mutable_context = (*execution_context).clone();
		let _batch_loop_span = debug_span!("insert_batch_loop").entered();
		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn, &mut mutable_context)?
		{
			let row_count = columns.row_count();
			let _rows_span = debug_span!("process_rows", row_count).entered();

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

					// Dictionary encoding: if column has a dictionary binding, encode the value
					let value = if let Some(dict_id) = rb_column.dictionary_id {
						let _dict_span = debug_span!("dictionary_encode").entered();
						let dictionary =
							CatalogStore::find_dictionary(std_txn.command_mut(), dict_id)?
								.ok_or_else(|| {
									internal_error!(
										"Dictionary {:?} not found for column {}",
										dict_id,
										rb_column.name
									)
								})?;
						let entry_id = std_txn
							.command_mut()
							.insert_into_dictionary(&dictionary, &value)?;
						entry_id.to_value()
					} else {
						value
					};

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
						Value::Duration(v) => layout.set_duration(&mut row, rb_idx, v),
						Value::RowNumber(_v) => {}
						Value::IdentityId(v) => layout.set_identity_id(&mut row, rb_idx, v),
						Value::Uuid4(v) => layout.set_uuid4(&mut row, rb_idx, v),
						Value::Uuid7(v) => layout.set_uuid7(&mut row, rb_idx, v),
						Value::Blob(v) => layout.set_blob(&mut row, rb_idx, &v),
						Value::Int(v) => layout.set_int(&mut row, rb_idx, &v),
						Value::Uint(v) => layout.set_uint(&mut row, rb_idx, &v),
						Value::Decimal(v) => layout.set_decimal(&mut row, rb_idx, &v),
						Value::Undefined => layout.set_undefined(&mut row, rb_idx),
						Value::Any(_) => {
							unreachable!("Any type cannot be stored in ring buffer")
						}
					}
				}

				// TODO: Check for primary key and handle upsert logic if needed

				use crate::transaction::operation::RingBufferOperations;

				// If buffer is full, delete the oldest entry first
				if metadata.is_full() {
					let oldest_row = RowNumber(metadata.head);
					std_txn.command_mut()
						.remove_from_ring_buffer(ring_buffer.clone(), oldest_row)?;
					// Advance head to next oldest item
					metadata.head += 1;
					metadata.count -= 1;
				}

				// Get next row number from sequence (monotonically increasing)
				let row_number = {
					let _seq_span = debug_span!("allocate_row_number").entered();
					RowSequence::next_row_number_for_ring_buffer(
						std_txn.command_mut(),
						ring_buffer.id,
					)?
				};

				// Store the row
				{
					let _store_span = debug_span!("store_row").entered();
					std_txn.command_mut().insert_into_ring_buffer_at(
						ring_buffer.clone(),
						row_number,
						row,
					)?;
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
		CatalogStore::update_ring_buffer_metadata(std_txn.command_mut(), metadata)?;

		// Return summary
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("ring_buffer", Value::Utf8(ring_buffer.name)),
			("inserted", Value::Uint8(inserted_count as u64)),
		]))
	}
}
