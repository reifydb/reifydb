// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::id::IndexId,
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedTable},
	},
	key::{EncodableKey, index_entry::IndexEntryKey, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_rql::plan::physical::UpdateTableNode;
use reifydb_transaction::standard::{StandardTransaction, command::StandardCommandTransaction};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
	},
	internal_error,
};
use reifydb_type::{fragment::Fragment, params::Params, return_error, value::{Value, r#type::Type}};

use super::primary_key;
use crate::{
	execute::{
		Batch, ExecutionContext, Executor, QueryNode, mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
	stack::Stack,
	transaction::operation::dictionary::DictionaryOperations,
};

impl Executor {
	pub(crate) fn update_table<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: UpdateTableNode,
		params: Params,
	) -> crate::Result<Columns> {
		// Get table from plan or infer from input pipeline
		let (namespace, table) = if let Some(target) = &plan.target {
			// Namespace and table explicitly specified
			let namespace_name = target.namespace().name();
			let Some(namespace) = self.catalog.find_namespace_by_name(txn, namespace_name)? else {
				return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
			};

			let Some(table) = self.catalog.find_table_by_name(txn, namespace.id, target.name())? else {
				let fragment = target.identifier().clone();
				return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
			};

			(namespace, table)
		} else {
			unimplemented!("Cannot infer target table from pipeline - no table found")
		};

		// Get or create schema with proper field names and constraints
		let schema = super::schema::get_or_create_table_schema(&self.catalog, &table, txn)?;

		// Create resolved source for the table
		let namespace_ident = Fragment::internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let table_ident = Fragment::internal(table.name.clone());
		let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
		let resolved_source = Some(ResolvedPrimitive::Table(resolved_table));

		let context = ExecutionContext {
			executor: self.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
			stack: Stack::new(),
		};

		let mut updated_count = 0;

		{
			let mut wrapped_txn = StandardTransaction::from(txn);
			let mut input_node = compile(*plan.input, &mut wrapped_txn, Arc::new(context.clone()));

			input_node.initialize(&mut wrapped_txn, &context)?;

			let mut mutable_context = context.clone();
			while let Some(Batch {
				columns,
			}) = input_node.next(&mut wrapped_txn, &mut mutable_context)?
			{
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
							Value::Undefined
						};

						let column_ident = Fragment::internal(&table_column.name);
						let resolved_column = ResolvedColumn::new(
							column_ident,
							context.source.clone().unwrap(),
							table_column.clone(),
						);

						value = coerce_value_to_column_type(
							value,
							table_column.constraint.get_type(),
							resolved_column,
							&context,
						)?;

						if let Err(e) = table_column.constraint.validate(&value) {
							return Err(e);
						}

						// Dictionary encoding: if column has a dictionary binding, encode the
						// value
						let value = if let Some(dict_id) = table_column.dictionary_id {
							let dictionary = self
								.catalog
								.find_dictionary(wrapped_txn.command_mut(), dict_id)?
								.ok_or_else(|| {
									internal_error!(
										"Dictionary {:?} not found for column {}",
										dict_id,
										table_column.name
									)
								})?;
							let entry_id = wrapped_txn
								.command_mut()
								.insert_into_dictionary(&dictionary, &value)?;
							entry_id.to_value()
						} else {
							value
						};

						schema.set_value(&mut row, table_idx, &value);
					}

					let row_number = row_numbers[row_numberx];

					let row_key = RowKey::encoded(table.id, row_number);

					if let Some(pk_def) = primary_key::get_primary_key(
						&self.catalog,
						wrapped_txn.command_mut(),
						&table,
					)? {
						if let Some(old_row_data) = wrapped_txn.command_mut().get(&row_key)? {
							let old_row = old_row_data.values;
							let old_key = primary_key::encode_primary_key(
								&pk_def, &old_row, &table, &schema,
							)?;

							wrapped_txn.command_mut().remove(&IndexEntryKey::new(
								table.id,
								IndexId::primary(pk_def.id),
								old_key,
							)
							.encode())?;
						}

						let new_key = primary_key::encode_primary_key(
							&pk_def, &row, &table, &schema,
						)?;

						let row_number_schema = Schema::testing(&[Type::Uint8]);
						let mut row_number_encoded = row_number_schema.allocate();
						row_number_schema.set_u64(
							&mut row_number_encoded,
							0,
							u64::from(row_number),
						);

						wrapped_txn.command_mut().set(
							&IndexEntryKey::new(
								table.id,
								IndexId::primary(pk_def.id),
								new_key,
							)
							.encode(),
							row_number_encoded,
						)?;
					}

					wrapped_txn.command_mut().set(&row_key, row)?;

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
}
