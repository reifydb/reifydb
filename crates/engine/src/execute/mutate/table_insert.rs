// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{
	CatalogStore,
	sequence::{ColumnSequence, RowSequence},
};
use reifydb_core::{
	interface::{
		EncodableKey, IndexEntryKey, IndexId, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
		Params, ResolvedColumn, ResolvedNamespace, ResolvedSource, ResolvedTable,
	},
	return_error,
	value::{
		column::Columns,
		encoded::{EncodedValues, EncodedValuesLayout},
	},
};
use reifydb_rql::plan::physical::InsertTableNode;
use reifydb_type::{
	Fragment, Type, Value,
	diagnostic::{catalog::table_not_found, index::primary_key_violation},
	internal_error,
};
use tracing::{debug_span, instrument};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{
		Batch, ExecutionContext, Executor, QueryNode, mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
	stack::Stack,
	transaction::operation::{DictionaryOperations, TableOperations},
	util::encode_value,
};

impl Executor {
	#[instrument(name = "mutate::table::insert", level = "trace", skip_all)]
	pub(crate) async fn insert_table<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: InsertTableNode,
		stack: &mut Stack,
	) -> crate::Result<Columns> {
		let namespace_name = plan.target.namespace().name();

		let namespace = CatalogStore::find_namespace_by_name(txn, namespace_name).await?.unwrap();

		let table_name = plan.target.name();
		let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, table_name).await? else {
			let fragment = plan.target.identifier().clone();
			return_error!(table_not_found(fragment.clone(), namespace_name, table_name,));
		};

		// Build storage layout types - use dictionary ID type for dictionary-encoded columns
		let mut table_types: Vec<Type> = Vec::new();
		for c in &table.columns {
			if let Some(dict_id) = c.dictionary_id {
				// For dictionary columns, we store the dictionary ID, not the original value
				// Look up the dictionary to get its ID type
				let dict_type = match CatalogStore::find_dictionary(txn, dict_id).await {
					Ok(Some(d)) => d.id_type,
					_ => c.constraint.get_type(),
				};
				table_types.push(dict_type);
			} else {
				table_types.push(c.constraint.get_type());
			}
		}
		let layout = EncodedValuesLayout::new(&table_types);

		// Create resolved source for the table
		let namespace_ident = Fragment::internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let table_ident = Fragment::internal(table.name.clone());
		let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
		let resolved_source = Some(ResolvedSource::Table(resolved_table));

		let execution_context = Arc::new(ExecutionContext {
			executor: self.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: Params::None,
			stack: stack.clone(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone());

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context).await?;

		// PASS 1: Validate and encode all rows first, before allocating any row numbers
		// This ensures we only allocate row numbers for valid rows (fail-fast on validation errors)
		let mut validated_rows: Vec<EncodedValues> = Vec::new();
		let mut mutable_context = (*execution_context).clone();

		let validate_span = debug_span!("validate_and_encode_rows").entered();
		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn, &mut mutable_context).await?
		{
			let row_count = columns.row_count();

			use std::collections::HashMap;
			let mut column_map: HashMap<&str, usize> = HashMap::new();
			for (idx, col) in columns.iter().enumerate() {
				column_map.insert(col.name().text(), idx);
			}

			for row_numberx in 0..row_count {
				let mut row = layout.allocate();

				// For each table column, find if it exists in the input columns
				for (table_idx, table_column) in table.columns.iter().enumerate() {
					let mut value =
						if let Some(&input_idx) = column_map.get(table_column.name.as_str()) {
							columns[input_idx].data().get_value(row_numberx)
						} else {
							Value::Undefined
						};

					// Handle auto-increment columns
					if table_column.auto_increment && matches!(value, Value::Undefined) {
						value = ColumnSequence::next_value(
							std_txn.command_mut(),
							table.id,
							table_column.id,
						)
						.await?;
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
						let _dict_span = debug_span!("dictionary_encode").entered();
						let dictionary =
							CatalogStore::find_dictionary(std_txn.command_mut(), dict_id)
								.await?
								.ok_or_else(|| {
									internal_error!(
										"Dictionary {:?} not found for column {}",
										dict_id,
										table_column.name
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

					encode_value(&layout, &mut row, table_idx, &value);
				}

				// Store the validated and encoded row for later insertion
				validated_rows.push(row);
			}
		}

		validate_span.exit();

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

		let row_numbers = {
			let _alloc_span = debug_span!("allocate_row_numbers", count = total_rows).entered();
			RowSequence::next_row_number_batch(std_txn.command_mut(), table.id, total_rows as u64).await?
		};

		assert_eq!(row_numbers.len(), validated_rows.len());

		// PASS 2: Insert all validated rows using the pre-allocated row numbers
		let _insert_span = debug_span!("insert_rows", count = total_rows).entered();
		for (row, &row_number) in validated_rows.iter().zip(row_numbers.iter()) {
			// Insert the row directly into storage
			std_txn.command_mut().insert_table(table.clone(), row.clone(), row_number).await?;

			// Store primary key index entry if table has one
			if let Some(pk_def) = primary_key::get_primary_key(std_txn.command_mut(), &table).await? {
				let index_key = primary_key::encode_primary_key(&pk_def, row, &table, &layout)?;

				// Check if primary key already exists
				let index_entry_key =
					IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key.clone());
				if std_txn.command_mut().contains_key(&index_entry_key.encode()).await? {
					let key_columns = pk_def.columns.iter().map(|c| c.name.clone()).collect();
					return_error!(primary_key_violation(
						plan.target.identifier().clone().into_owned(),
						table.name.clone(),
						key_columns,
					));
				}

				// Store the index entry with the row number as value
				let row_number_layout = EncodedValuesLayout::new(&[Type::Uint8]);
				let mut row_number_encoded = row_number_layout.allocate();
				row_number_layout.set_u64(&mut row_number_encoded, 0, u64::from(row_number));

				std_txn.command_mut().set(&index_entry_key.encode(), row_number_encoded).await?;
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("table", Value::Utf8(table.name)),
			("inserted", Value::Uint8(total_rows as u64)),
		]))
	}
}
