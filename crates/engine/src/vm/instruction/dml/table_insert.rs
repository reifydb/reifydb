// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{encoded::EncodedValues, schema::Schema},
	error::diagnostic::{catalog::table_not_found, index::primary_key_violation},
	interface::{
		catalog::id::IndexId,
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedTable},
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::InsertTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, r#type::Type},
};
use tracing::instrument;

use super::{primary_key, schema::get_or_create_table_schema};
use crate::{
	transaction::operation::{dictionary::DictionaryOperations, table::TableOperations},
	vm::{
		instruction::dml::coerce::coerce_value_to_column_type,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

#[instrument(name = "mutate::table::insert", level = "trace", skip_all)]
pub(crate) fn insert_table<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertTableNode,
	stack: &mut SymbolTable,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();

	let namespace = services.catalog.find_namespace_by_name(txn, namespace_name)?.unwrap();

	let table_name = plan.target.name();
	let Some(table) = services.catalog.find_table_by_name(txn, namespace.id, table_name)? else {
		let fragment = plan.target.identifier().clone();
		return_error!(table_not_found(fragment.clone(), namespace_name, table_name,));
	};

	// Get or create schema with proper field names and constraints
	let schema = get_or_create_table_schema(&services.catalog, &table, txn)?;

	// Create resolved source for the table
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let table_ident = Fragment::internal(table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
	let resolved_source = Some(ResolvedPrimitive::Table(resolved_table));

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: Params::None,
		stack: stack.clone(),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	// Initialize the operator before execution
	input_node.initialize(txn, &execution_context)?;

	// PASS 1: Validate and encode all rows first, before allocating any row numbers
	// This ensures we only allocate row numbers for valid rows (fail-fast on validation errors)
	let mut validated_rows: Vec<EncodedValues> = Vec::new();
	let mut mutable_context = (*execution_context).clone();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.row_count();

		use std::collections::HashMap;
		let mut column_map: HashMap<&str, usize> = HashMap::new();
		for (idx, col) in columns.iter().enumerate() {
			column_map.insert(col.name().text(), idx);
		}

		for row_numberx in 0..row_count {
			let mut row = schema.allocate();

			// For each table column, find if it exists in the input columns
			for (table_idx, table_column) in table.columns.iter().enumerate() {
				let mut value = if let Some(&input_idx) = column_map.get(table_column.name.as_str()) {
					columns[input_idx].data().get_value(row_numberx)
				} else {
					Value::None
				};

				// Handle auto-increment columns
				if table_column.auto_increment && matches!(value, Value::None) {
					value = services.catalog.column_sequence_next_value(
						txn,
						table.id,
						table_column.id,
					)?;
				}

				// Create ResolvedColumn for this column
				let column_ident = Fragment::internal(table_column.name.clone());
				let resolved_column = ResolvedColumn::new(
					column_ident,
					execution_context.source.clone().unwrap(),
					table_column.clone(),
				);

				value = coerce_value_to_column_type(
					value,
					table_column.constraint.get_type(),
					resolved_column,
					&execution_context,
				)?;

				// Validate the value against the column's constraint
				if let Err(e) = table_column.constraint.validate(&value) {
					return Err(e);
				}

				// Dictionary encoding: if column has a dictionary binding, encode the value
				let value = if let Some(dict_id) = table_column.dictionary_id {
					let dictionary =
						services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
							internal_error!(
								"Dictionary {:?} not found for column {}",
								dict_id,
								table_column.name
							)
						})?;
					let entry_id = txn.insert_into_dictionary(&dictionary, &value)?;
					entry_id.to_value()
				} else {
					value
				};

				schema.set_value(&mut row, table_idx, &value);
			}

			// Store the validated and encoded row for later insertion
			validated_rows.push(row);
		}
	}

	// BATCH ALLOCATION: Now that all rows are validated, allocate row numbers in one batch
	let total_rows = validated_rows.len();
	if total_rows == 0 {
		// No rows to insert, return early
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("table", Value::Utf8(table.name)),
			("inserted", Value::Uint8(0)),
		]));
	}

	let row_numbers = services.catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;

	assert_eq!(row_numbers.len(), validated_rows.len());

	// PASS 2: Insert all validated rows using the pre-allocated row numbers
	for (row, &row_number) in validated_rows.iter().zip(row_numbers.iter()) {
		// Insert the row directly into storage
		txn.insert_table(table.clone(), row.clone(), row_number)?;

		// Store primary key index entry if table has one
		if let Some(pk_def) = primary_key::get_primary_key(&services.catalog, txn, &table)? {
			let index_key = primary_key::encode_primary_key(&pk_def, row, &table, &schema)?;

			// Check if primary key already exists
			let index_entry_key =
				IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key.clone());
			if txn.contains_key(&index_entry_key.encode())? {
				let key_columns = pk_def.columns.iter().map(|c| c.name.clone()).collect();
				return_error!(primary_key_violation(
					plan.target.identifier().clone(),
					table.name.clone(),
					key_columns,
				));
			}

			// Store the index entry with the row number as value
			let row_number_schema = Schema::testing(&[Type::Uint8]);
			let mut row_number_encoded = row_number_schema.allocate();
			row_number_schema.set_u64(&mut row_number_encoded, 0, u64::from(row_number));

			txn.set(&index_entry_key.encode(), row_number_encoded)?;
		}
	}

	// Return summary columns
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("table", Value::Utf8(table.name)),
		("inserted", Value::Uint8(total_rows as u64)),
	]))
}
