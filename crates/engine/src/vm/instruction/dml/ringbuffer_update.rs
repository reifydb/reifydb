// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, ringbuffer_not_found},
		engine,
	},
	interface::{
		catalog::policy::PolicyTargetType,
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedRingBuffer},
	},
	internal_error,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::UpdateRingBufferNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId},
};

use super::{coerce::coerce_value_to_column_type, schema::get_or_create_ringbuffer_schema};
use crate::{
	Result,
	policy::PolicyEvaluator,
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

pub(crate) fn update_ringbuffer<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateRingBufferNode,
	params: Params,
	identity: IdentityId,
	symbol_table_ref: &SymbolTable,
) -> Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let ringbuffer_name = plan.target.name();
	let Some(ringbuffer) = services.catalog.find_ringbuffer_by_name(txn, namespace.id, ringbuffer_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(ringbuffer_not_found(fragment.clone(), namespace_name, ringbuffer_name));
	};

	// Get current metadata - we need it to validate that rows exist
	let Some(metadata) = services.catalog.find_ringbuffer_metadata(txn, ringbuffer.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(ringbuffer_not_found(fragment, namespace_name, ringbuffer_name));
	};

	// Get or create schema with proper field names and constraints
	let schema = get_or_create_ringbuffer_schema(&services.catalog, &ringbuffer, txn)?;

	// Create resolved source for the ring buffer
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let rb_ident = Fragment::internal(ringbuffer.name.clone());
	let resolved_rb = ResolvedRingBuffer::new(rb_ident, resolved_namespace, ringbuffer.clone());
	let resolved_source = Some(ResolvedPrimitive::RingBuffer(resolved_rb));

	// Create execution context
	let context = QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		stack: SymbolTable::new(),
		identity: IdentityId::root(),
	};

	let mut updated_count = 0;

	// Process all input batches
	{
		let mut input_node = compile(*plan.input, txn, Arc::new(context.clone()));

		// Initialize the operator before execution
		input_node.initialize(txn, &context)?;

		let mut mutable_context = context.clone();
		while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
			// Enforce write policies before processing rows
			PolicyEvaluator::new(services, symbol_table_ref).enforce_write_policies(
				txn,
				identity,
				&namespace.name,
				&ringbuffer.name,
				"update",
				&columns,
				PolicyTargetType::RingBuffer,
			)?;

			// Get encoded numbers from the Columns structure
			if columns.row_numbers.is_empty() {
				return_error!(engine::missing_row_number_column());
			}

			// Extract RowNumber data
			let row_numbers = &columns.row_numbers;

			let row_count = columns.row_count();

			let mut column_map: HashMap<&str, usize> = HashMap::new();
			for (idx, col) in columns.iter().enumerate() {
				column_map.insert(col.name().text(), idx);
			}

			for row_idx in 0..row_count {
				let mut row = schema.allocate();

				// For each ring buffer column, find if it exists in the input columns
				for (rb_idx, rb_column) in ringbuffer.columns.iter().enumerate() {
					let mut value =
						if let Some(&input_idx) = column_map.get(rb_column.name.as_str()) {
							columns[input_idx].data().get_value(row_idx)
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
					if let Err(mut e) = rb_column.constraint.validate(&value) {
						e.0.fragment = column_ident.clone();
						return Err(e);
					}

					// Dictionary encoding: if column has a dictionary binding, encode the
					// value
					let value = if let Some(dict_id) = rb_column.dictionary_id {
						let dictionary = services
							.catalog
							.find_dictionary(txn, dict_id)?
							.ok_or_else(|| {
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

				// Check if row is in the valid occupied range [head, tail)
				let is_occupied = row_number.0 >= metadata.head && row_number.0 < metadata.tail;

				if !is_occupied {
					continue;
				}

				// Update the encoded using interceptors
				txn.update_ringbuffer(ringbuffer.clone(), row_number, row)?;

				updated_count += 1;
			}
		}
	}

	// Return summary columns
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("ringbuffer", Value::Utf8(ringbuffer.name)),
		("updated", Value::Uint8(updated_count as u64)),
	]))
}
