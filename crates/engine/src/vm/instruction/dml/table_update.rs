// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::schema::Schema,
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
	},
	interface::{
		catalog::{id::IndexId, policy::PolicyTargetType},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedTable},
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::UpdateTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, r#type::Type},
};

use super::primary_key;
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

pub(crate) fn update_table<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateTableNode,
	params: Params,
	identity: IdentityId,
	symbol_table_ref: &crate::vm::stack::SymbolTable,
) -> crate::Result<Columns> {
	// Get table from plan or infer from input pipeline
	let (namespace, table) = if let Some(target) = &plan.target {
		// Namespace and table explicitly specified
		let namespace_name = target.namespace().name();
		let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
			return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
		};

		let Some(table) = services.catalog.find_table_by_name(txn, namespace.id, target.name())? else {
			let fragment = target.identifier().clone();
			return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
		};

		(namespace, table)
	} else {
		unimplemented!("Cannot infer target table from pipeline - no table found")
	};

	// Get or create schema with proper field names and constraints
	let schema = super::schema::get_or_create_table_schema(&services.catalog, &table, txn)?;

	// Create resolved source for the table
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let table_ident = Fragment::internal(table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
	let resolved_source = Some(ResolvedPrimitive::Table(resolved_table));

	let context = QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		stack: SymbolTable::new(),
		identity: IdentityId::root(),
	};

	let mut updated_count = 0;

	{
		let mut input_node = compile(*plan.input, txn, Arc::new(context.clone()));

		input_node.initialize(txn, &context)?;

		let mut mutable_context = context.clone();
		while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
			// Enforce write policies before processing rows
			crate::policy::enforce_write_policies(
				services,
				txn,
				identity,
				&namespace.name,
				&table.name,
				"update",
				&columns,
				symbol_table_ref,
				PolicyTargetType::Table,
			)?;

			if columns.row_numbers.is_empty() {
				return_error!(engine::missing_row_number_column());
			}

			let row_numbers = &columns.row_numbers;

			let row_count = columns.row_count();

			for row_numberx in 0..row_count {
				let mut row = schema.allocate();

				for (table_idx, table_column) in table.columns.iter().enumerate() {
					let mut value = if let Some(input_column) =
						columns.iter().find(|col| col.name() == table_column.name)
					{
						input_column.data().get_value(row_numberx)
					} else {
						Value::none()
					};

					let column_ident = columns
						.iter()
						.find(|col| col.name() == table_column.name)
						.map(|col| col.name().clone())
						.unwrap_or_else(|| Fragment::internal(&table_column.name));
					let resolved_column = ResolvedColumn::new(
						column_ident.clone(),
						context.source.clone().unwrap(),
						table_column.clone(),
					);

					value = coerce_value_to_column_type(
						value,
						table_column.constraint.get_type(),
						resolved_column,
						&context,
					)?;

					if let Err(mut e) = table_column.constraint.validate(&value) {
						e.0.fragment = column_ident.clone();
						return Err(e);
					}

					// Dictionary encoding: if column has a dictionary binding, encode the
					// value
					let value = if let Some(dict_id) = table_column.dictionary_id {
						let dictionary = services
							.catalog
							.find_dictionary(txn, dict_id)?
							.ok_or_else(|| {
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

				let row_number = row_numbers[row_numberx];

				let row_key = RowKey::encoded(table.id, row_number);

				if let Some(pk_def) = primary_key::get_primary_key(&services.catalog, txn, &table)? {
					if let Some(old_row_data) = txn.get(&row_key)? {
						let old_row = old_row_data.values;
						let old_key = primary_key::encode_primary_key(
							&pk_def, &old_row, &table, &schema,
						)?;

						txn.remove(&IndexEntryKey::new(
							table.id,
							IndexId::primary(pk_def.id),
							old_key,
						)
						.encode())?;
					}

					let new_key = primary_key::encode_primary_key(&pk_def, &row, &table, &schema)?;

					let row_number_schema = Schema::testing(&[Type::Uint8]);
					let mut row_number_encoded = row_number_schema.allocate();
					row_number_schema.set_u64(&mut row_number_encoded, 0, u64::from(row_number));

					txn.set(
						&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), new_key)
							.encode(),
						row_number_encoded,
					)?;
				}

				txn.update_table(table.clone(), row_number, row)?;

				updated_count += 1;
			}
		}
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("table", Value::Utf8(table.name)),
		("updated", Value::Uint8(updated_count as u64)),
	]))
}
