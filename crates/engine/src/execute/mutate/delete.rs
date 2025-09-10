// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::Bound::Included, sync::Arc};

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	EncodedKeyRange,
	interface::{
		EncodableKey, EncodableKeyRange, GetEncodedRowLayout,
		IndexEntryKey, IndexId, Params, RowKey, RowKeyRange,
		Transaction, VersionedCommandTransaction,
		VersionedQueryTransaction,
	},
	value::columnar::{ColumnData, Columns},
};
use reifydb_rql::plan::{
	logical::extract_table_from_plan, physical::DeletePlan,
};
use reifydb_type::{
	ROW_NUMBER_COLUMN_NAME, Value,
	diagnostic::{
		catalog::{schema_not_found, table_not_found},
		engine,
	},
	return_error,
};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{
		Batch, ExecutionContext, Executor, QueryNode,
		query::compile::compile,
	},
};

impl Executor {
	pub(crate) fn delete<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: DeletePlan,
		params: Params,
	) -> crate::Result<Columns> {
		// Get table from plan or infer from input pipeline
		let (schema, table) = if let Some(target) = &plan.target {
			// Schema and table explicitly specified
			let schema_name = target.schema.text();
			let Some(schema) = CatalogStore::find_schema_by_name(
				txn,
				schema_name,
			)?
			else {
				return_error!(schema_not_found(
					Some(target
						.schema
						.clone()
						.into_owned()),
					schema_name
				));
			};

			let Some(table) = CatalogStore::find_table_by_name(
				txn,
				schema.id,
				target.name.text(),
			)?
			else {
				let fragment = target.name.clone();
				return_error!(table_not_found(
					fragment.clone(),
					schema_name,
					target.name.text(),
				));
			};

			(schema, table)
		} else {
			// Both should be inferred from the pipeline
			// Extract table info from the input plan if it
			// exists
			if let Some(input_plan) = &plan.input {
				extract_table_from_plan(input_plan).expect(
					"Cannot infer target table from pipeline - no table found",
				)
			} else {
				panic!(
					"DELETE without input requires explicit target table"
				);
			}
		};

		let mut deleted_count = 0;

		if let Some(input_plan) = plan.input {
			// Delete specific rows based on input plan
			// First collect all row numbers to delete
			let mut row_numbers_to_delete = Vec::new();

			let mut std_txn = StandardTransaction::from(txn);
			let mut input_node = compile(
				*input_plan,
				&mut std_txn,
				Arc::new(ExecutionContext {
					functions: self.functions.clone(),
					table: Some(table.clone()),
					batch_size: 1024,
					preserve_row_numbers: true,
					params: params.clone(),
				}),
			);

			let context = ExecutionContext {
				functions: self.functions.clone(),
				table: Some(table.clone()),
				batch_size: 1024,
				preserve_row_numbers: true,
				params: params.clone(),
			};

			// Initialize the node before execution
			input_node.initialize(&mut std_txn, &context)?;

			while let Some(Batch {
				columns,
			}) = input_node.next(&mut std_txn)?
			{
				// Find the RowNumber column - return error if
				// not found
				let Some(row_number_column) =
					columns.iter().find(|col| {
						col.name() == ROW_NUMBER_COLUMN_NAME
					})
				else {
					return_error!(
						engine::missing_row_number_column()
					);
				};

				// Extract RowNumber data - return error if any
				// are undefined
				let row_numbers = match &row_number_column
					.data()
				{
					ColumnData::RowNumber(container) => {
						// Check that all row IDs are
						// defined
						for i in 0..container
							.data()
							.len()
						{
							if !container
								.is_defined(i)
							{
								return_error!(engine::invalid_row_number_values());
							}
						}
						container.data()
					}
					_ => return_error!(
						engine::invalid_row_number_values()
					),
				};

				for row_numberx in 0..columns.row_count() {
					let row_number =
						row_numbers[row_numberx];
					row_numbers_to_delete.push(row_number);
				}
			}

			// Get primary key info if table has one
			let pk_def = primary_key::get_primary_key(
				std_txn.command_mut(),
				&table,
			)?;

			let cmd = std_txn.command();
			for row_number in row_numbers_to_delete {
				let row_key = RowKey {
					source: table.id.into(),
					row: row_number,
				}
				.encode();

				// Remove primary key index entry if table has
				// one
				if let Some(ref pk_def) = pk_def {
					if let Some(row_data) =
						cmd.get(&row_key)?
					{
						let row = row_data.row;
						let layout = table.get_layout();
						let index_key = primary_key::encode_primary_key(
							pk_def,
							&row,
							&table,
							&layout,
						)?;

						cmd
							.remove(&IndexEntryKey::new(
							table.id,
							IndexId::primary(
								pk_def.id,
							),
							index_key,
						)
						.encode())?;
					}
				}

				// Now remove the row
				cmd.remove(&row_key)?;
				deleted_count += 1;
			}
		} else {
			// Delete entire table - scan all rows and delete them
			let range = RowKeyRange {
				source: table.id.into(),
			};

			// Get primary key info if table has one
			let pk_def = primary_key::get_primary_key(txn, &table)?;

			let rows = txn
				.range(EncodedKeyRange::new(
					Included(range.start().unwrap()),
					Included(range.end().unwrap()),
				))?
				.collect::<Vec<_>>();

			for versioned in rows {
				// Remove primary key index entry if table has
				// one
				if let Some(ref pk_def) = pk_def {
					let layout = table.get_layout();
					let index_key = super::primary_key::encode_primary_key(
						pk_def,
						&versioned.row,
						&table,
						&layout,
					)?;

					txn.remove(&IndexEntryKey::new(
						table.id,
						IndexId::primary(pk_def.id),
						index_key,
					)
					.encode())?;
				}

				// Remove the row
				txn.remove(&versioned.key)?;
				deleted_count += 1;
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("schema", Value::Utf8(schema.name)),
			("table", Value::Utf8(table.name)),
			("deleted", Value::Uint8(deleted_count as u64)),
		]))
	}
}
