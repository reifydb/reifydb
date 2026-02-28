// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::catalog::{namespace_not_found, ringbuffer_not_found},
	interface::{
		catalog::policy::PolicyTargetType,
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedRingBuffer},
	},
	internal_error,
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

use super::coerce::coerce_value_to_column_type;
use crate::{
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
pub(crate) fn insert_ringbuffer<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertRingBufferNode,
	params: Params,
	identity: IdentityId,
	symbol_table: &SymbolTable,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let ringbuffer_name = plan.target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id, ringbuffer_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};

	// Get current metadata
	let Some(mut metadata) = services.catalog.find_ringbuffer_metadata(txn, ringbuffer.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(ringbuffer_not_found(fragment, namespace_name, ringbuffer_name));
	};

	// Get or create schema with proper field names and constraints
	let schema = super::schema::get_or_create_ringbuffer_schema(&services.catalog, &ringbuffer, txn)?;

	// Create resolved source for the ring buffer
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	let resolved_source = Some(ResolvedPrimitive::RingBuffer(resolved_rb));

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		stack: SymbolTable::new(),
		identity: IdentityId::root(),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	let mut inserted_count = 0;

	// Initialize the operator before execution
	input_node.initialize(txn, &execution_context)?;

	// Process all input batches
	let mut mutable_context = (*execution_context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		// Enforce write policies before processing rows
		crate::policy::enforce_write_policies(
			services,
			txn,
			identity,
			namespace_name,
			ringbuffer_name,
			"insert",
			&columns,
			symbol_table,
			PolicyTargetType::RingBuffer,
		)?;

		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			let mut row = schema.allocate();

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

				schema.set_value(&mut row, rb_idx, &value);
			}

			// If buffer is full, delete the oldest entry first
			if metadata.is_full() {
				let oldest_row = RowNumber(metadata.head);
				txn.remove_from_ringbuffer(ringbuffer.clone(), oldest_row)?;
				// Advance head to next oldest item
				metadata.head += 1;
				metadata.count -= 1;
			}

			// Get next row number from sequence (monotonically increasing)
			let row_number = services.catalog.next_row_number_for_ringbuffer(txn, ringbuffer.id)?;

			// Store the row
			txn.insert_ringbuffer_at(ringbuffer.clone(), row_number, row)?;

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
	services.catalog.update_ringbuffer_metadata_txn(txn, metadata)?;

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("ringbuffer", Value::Utf8(ringbuffer.name)),
		("inserted", Value::Uint8(inserted_count as u64)),
	]))
}
