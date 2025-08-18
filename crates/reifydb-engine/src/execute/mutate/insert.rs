// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{
	Catalog,
	sequence::{TableColumnSequence, TableRowSequence},
};
use reifydb_core::{
	ColumnDescriptor, IntoOwnedFragment, Type, Value,
	interface::{
		ActiveCommandTransaction, ColumnPolicyKind, EncodableKey,
		Params, TableRowKey, Transaction, VersionedCommandTransaction,
	},
	result::error::diagnostic::catalog::table_not_found,
	return_error,
	row::EncodedRowLayout,
};
use reifydb_rql::plan::physical::InsertPlan;

use crate::{
	columnar::Columns,
	execute::{
		Batch, ExecutionContext, Executor, compile,
		mutate::coerce::coerce_value_to_column_type,
	},
};

impl<T: Transaction> Executor<T> {
	pub(crate) fn insert(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		plan: InsertPlan,
		params: Params,
	) -> crate::Result<Columns> {
		let schema_name = plan
			.schema
			.as_ref()
			.map(|s| s.fragment())
			.unwrap(); // FIXME

		let schema =
			Catalog::get_schema_by_name(txn, schema_name)?.unwrap();
		let Some(table) = Catalog::get_table_by_name(
			txn,
			schema.id,
			&plan.table.fragment(),
		)?
		else {
			let fragment = plan.table.into_fragment();
			return_error!(table_not_found(
				fragment.clone(),
				schema_name,
				&fragment.fragment(),
			));
		};

		let table_types: Vec<Type> =
			table.columns.iter().map(|c| c.ty).collect();
		let layout = EncodedRowLayout::new(&table_types);

		let execution_context = Arc::new(ExecutionContext {
			functions: self.functions.clone(),
			table: Some(table.clone()),
			batch_size: 1024,
			preserve_row_ids: false,
			params: params.clone(),
		});

		let mut input_node =
			compile(*plan.input, txn, execution_context.clone());

		let mut inserted_count = 0;

		// Process all input batches using volcano iterator pattern
		while let Some(Batch {
			columns,
		}) = input_node.next(&execution_context, txn)?
		{
			let row_count = columns.row_count();

			for row_idx in 0..row_count {
				let mut row = layout.allocate_row();

				// For each table column, find if it exists in
				// the input columns
				for (table_idx, table_column) in
					table.columns.iter().enumerate()
				{
					let mut value =
						if let Some(input_column) =
							columns.iter().find(
								|col| {
									col.name() == table_column.name
								},
							) {
							input_column
								.data()
								.get_value(
									row_idx,
								)
						} else {
							Value::Undefined
						};

					// Handle auto-increment columns
					if table_column.auto_increment
						&& matches!(
							value,
							Value::Undefined
						) {
						value = TableColumnSequence::next_value(txn, table.id, table_column.id)?;
					}

					let policies: Vec<ColumnPolicyKind> =
						table_column
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
						&execution_context,
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
						Value::RowId(_v) => {}
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

				// Insert the row into the database
				let row_id = TableRowSequence::next_row_id(
					txn, table.id,
				)?;
				txn.set(
					&TableRowKey {
						table: table.id,
						row: row_id,
					}
					.encode(),
					row,
				)
				.unwrap();

				inserted_count += 1;
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("schema", Value::Utf8(schema.name)),
			("table", Value::Utf8(table.name)),
			("inserted", Value::Uint8(inserted_count as u64)),
		]))
	}
}
