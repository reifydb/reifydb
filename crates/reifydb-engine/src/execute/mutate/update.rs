// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	ColumnDescriptor, Type, Value,
	interface::{
		ColumnPolicyKind, EncodableKey, Params, RowKey, Transaction,
		VersionedCommandTransaction,
	},
	result::error::diagnostic::{
		catalog::{schema_not_found, table_not_found},
		engine,
	},
	return_error,
	row::EncodedRowLayout,
	value::row_number::ROW_NUMBER_COLUMN_NAME,
};
use reifydb_rql::plan::{
	logical::extract_table_from_plan, physical::UpdatePlan,
};

use crate::{
	StandardCommandTransaction, StandardTransaction,
	columnar::{ColumnData, Columns},
	execute::{
		Batch, ExecutionContext, Executor,
		mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
};

impl Executor {
	pub(crate) fn update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: UpdatePlan,
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
				extract_table_from_plan(&plan.input).expect(
					"Cannot infer target table from pipeline - no table found",
				)
			} else {
				// Mixed case - one specified, one not
				// (shouldn't happen with current parser)
				panic!(
					"UPDATE requires either both schema and table or neither"
				);
			};

		let table_types: Vec<Type> =
			table.columns.iter().map(|c| c.ty).collect();
		let layout = EncodedRowLayout::new(&table_types);

		// Create execution context
		let context = ExecutionContext {
			functions: self.functions.clone(),
			table: Some(table.clone()),
			batch_size: 1024,
			preserve_row_numbers: true,
			params: params.clone(),
		};

		let mut updated_count = 0;

		// Process all input batches - we need to handle compilation and
		// execution with proper transaction borrowing
		{
			let mut wrapped_txn = StandardTransaction::from(txn);
			let mut input_node = compile(
				*plan.input,
				&mut wrapped_txn,
				Arc::new(context.clone()),
			);

			while let Some(Batch {
				columns,
			}) = input_node.next(&context, &mut wrapped_txn)?
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

				// Extract RowNumber data - panic if any are
				// undefined
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

				let row_count = columns.row_count();

				for row_numberx in 0..row_count {
					let mut row = layout.allocate_row();

					// For each table column, find if it
					// exists in the input columns
					for (table_idx, table_column) in
						table.columns.iter().enumerate()
					{
						let mut value = if let Some(
							input_column,
						) = columns
							.iter()
							.find(|col| {
								col.name() == table_column.name
							}) {
							input_column
								.data()
								.get_value(
								row_numberx,
							)
						} else {
							Value::Undefined
						};

						// Apply automatic type coercion
						// Extract policies (no
						// conversion needed since
						// types are now unified)
						let policies: Vec<
							ColumnPolicyKind,
						> = table_column
							.policies
							.iter()
							.map(|cp| {
								cp.policy
									.clone()
							})
							.collect();

						value = coerce_value_to_column_type(
						value,
						table_column.ty,
						ColumnDescriptor::new()
							.with_schema(
								&schema.name,
							)
							.with_table(&table.name)
							.with_column(
								&table_column
									.name,
							)
							.with_column_type(
								table_column.ty,
							)
							.with_policies(
								policies,
							),
						&context,
					)?;

						match value {
						Value::Bool(v) => layout
							.set_bool(
								&mut row,
								table_idx, v,
							),
						Value::Float4(v) => layout
							.set_f32(
								&mut row,
								table_idx, *v,
							),
						Value::Float8(v) => layout
							.set_f64(
								&mut row,
								table_idx, *v,
							),
						Value::Int1(v) => layout
							.set_i8(
								&mut row,
								table_idx, v,
							),
						Value::Int2(v) => layout
							.set_i16(
								&mut row,
								table_idx, v,
							),
						Value::Int4(v) => layout
							.set_i32(
								&mut row,
								table_idx, v,
							),
						Value::Int8(v) => layout
							.set_i64(
								&mut row,
								table_idx, v,
							),
						Value::Int16(v) => layout
							.set_i128(
								&mut row,
								table_idx, v,
							),
						Value::Utf8(v) => layout
							.set_utf8(
								&mut row,
								table_idx, v,
							),
						Value::Uint1(v) => layout
							.set_u8(
								&mut row,
								table_idx, v,
							),
						Value::Uint2(v) => layout
							.set_u16(
								&mut row,
								table_idx, v,
							),
						Value::Uint4(v) => layout
							.set_u32(
								&mut row,
								table_idx, v,
							),
						Value::Uint8(v) => layout
							.set_u64(
								&mut row,
								table_idx, v,
							),
						Value::Uint16(v) => layout
							.set_u128(
								&mut row,
								table_idx, v,
							),
						Value::Date(v) => layout
							.set_date(
								&mut row,
								table_idx, v,
							),
						Value::DateTime(v) => layout
							.set_datetime(
								&mut row,
								table_idx, v,
							),
						Value::Time(v) => layout
							.set_time(
								&mut row,
								table_idx, v,
							),
						Value::Interval(v) => layout
							.set_interval(
								&mut row,
								table_idx, v,
							),
						Value::RowNumber(_v) => {}
						Value::IdentityId(v) => layout
							.set_identity_id(
								&mut row,
								table_idx, v,
							),
						Value::Uuid4(v) => layout
							.set_uuid4(
								&mut row,
								table_idx, v,
							),
						Value::Uuid7(v) => layout
							.set_uuid7(
								&mut row,
								table_idx, v,
							),
						Value::Blob(v) => layout
							.set_blob(
								&mut row,
								table_idx, &v,
							),
						Value::Undefined => layout
							.set_undefined(
								&mut row,
								table_idx,
							),
					}
					}

					// Update the row using the existing
					// RowNumber from the columns
					let row_number =
						row_numbers[row_numberx];
					wrapped_txn.command_mut().set(
						&RowKey {
							store: table.id.into(),
							row: row_number,
						}
						.encode(),
						row,
					)?;

					updated_count += 1;
				}
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("schema", Value::Utf8(schema.name)),
			("table", Value::Utf8(table.name)),
			("updated", Value::Uint8(updated_count as u64)),
		]))
	}
}
