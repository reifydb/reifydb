// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::Bound::Included, sync::Arc};

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	EncodedKeyRange, Value,
	interface::{
		EncodableKey, EncodableKeyRange, Params, TableRowKey,
		TableRowKeyRange, Transaction, VersionedCommandTransaction,
		VersionedQueryTransaction,
	},
	result::error::diagnostic::{
		catalog::{schema_not_found, table_not_found},
		engine,
	},
	return_error,
	value::row_number::ROW_NUMBER_COLUMN_NAME,
};
use reifydb_rql::plan::{
	logical::extract_table_from_plan, physical::DeletePlan,
};

use crate::{
	StandardCommandTransaction,
	columnar::{ColumnData, Columns},
	execute::{Batch, ExecutionContext, Executor, compile},
};

impl Executor {
	pub(crate) fn delete<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: DeletePlan,
		params: Params,
	) -> crate::Result<Columns> {
		// Get table from plan or infer from input pipeline
		let (schema, table) =
			if let (Some(schema_ref), Some(table_ref)) =
				(&plan.schema, &plan.table)
			{
				// Both schema and table explicitly specified
				let schema_name = schema_ref.fragment();
				let Some(schema) =
					CatalogStore::find_schema_by_name(
						txn,
						schema_name,
					)?
				else {
					return_error!(schema_not_found(
						Some(schema_ref
							.clone()
							.into_owned()),
						schema_name
					));
				};

				let Some(table) =
					CatalogStore::find_table_by_name(
						txn,
						schema.id,
						&table_ref.fragment(),
					)?
				else {
					let fragment = table_ref.clone();
					return_error!(table_not_found(
						fragment.clone(),
						schema_name,
						&fragment.fragment(),
					));
				};

				(schema, table)
			} else if plan.schema.is_none() && plan.table.is_none()
			{
				// Both should be inferred from the pipeline
				// Extract table info from the input plan if it
				// exists
				if let Some(input_plan) = &plan.input {
					extract_table_from_plan(input_plan)
						.expect(
							"Cannot infer target table from pipeline - no table found",
						)
				} else {
					panic!(
						"DELETE without input requires explicit target table"
					);
				}
			} else {
				// Mixed case - one specified, one not
				// (shouldn't happen with current parser)
				panic!(
					"DELETE requires either both schema and table or neither"
				);
			};

		let mut deleted_count = 0;

		if let Some(input_plan) = plan.input {
			// Delete specific rows based on input plan
			let mut input_node = compile(
				*input_plan,
				txn,
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

			while let Some(Batch {
				columns,
			}) = input_node.next(&context, txn)?
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
					txn.remove(&TableRowKey {
						table: table.id,
						row: row_number,
					}
					.encode())?;
					deleted_count += 1;
				}
			}
		} else {
			// Delete entire table - scan all rows and delete them
			let range = TableRowKeyRange {
				table: table.id,
			};

			let keys = txn
				.range(EncodedKeyRange::new(
					Included(range.start().unwrap()),
					Included(range.end().unwrap()),
				))?
				.map(|versioned| versioned.key)
				.collect::<Vec<_>>();
			for key in keys {
				txn.remove(&key)?;
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
