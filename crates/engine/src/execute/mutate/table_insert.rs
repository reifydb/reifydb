// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, sequence::ColumnSequence};
use reifydb_core::{
	interface::{
		EncodableKey, IndexEntryKey, IndexId, MultiVersionCommandTransaction, Params, ResolvedColumn,
		ResolvedNamespace, ResolvedSource, ResolvedTable, Transaction,
	},
	return_error,
	value::{column::Columns, row::EncodedRowLayout},
};
use reifydb_rql::plan::physical::InsertTableNode;
use reifydb_type::{Fragment, Type, Value, diagnostic::catalog::table_not_found};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{
		Batch, ExecutionContext, Executor, QueryNode, mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
	transaction::operation::TableOperations,
};

impl Executor {
	pub(crate) fn insert_table<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: InsertTableNode<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		let namespace_name = plan.target.namespace().name();

		let namespace = CatalogStore::find_namespace_by_name(txn, namespace_name)?.unwrap();

		let table_name = plan.target.name();
		let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, table_name)? else {
			let fragment = plan.target.identifier().clone();
			return_error!(table_not_found(fragment.clone(), namespace_name, table_name,));
		};

		let table_types: Vec<Type> = table.columns.iter().map(|c| c.constraint.get_type()).collect();
		let layout = EncodedRowLayout::new(&table_types);

		// Create resolved source for the table
		let namespace_ident = Fragment::owned_internal(namespace.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

		let table_ident = Fragment::owned_internal(table.name.clone());
		let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
		let resolved_source = Some(ResolvedSource::Table(resolved_table));

		let execution_context = Arc::new(ExecutionContext {
			functions: self.functions.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone());

		let mut inserted_count = 0;

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context)?;

		// Process all input batches using volcano iterator pattern
		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn)?
		{
			let row_count = columns.row_count();

			for row_numberx in 0..row_count {
				let mut row = layout.allocate_row();

				// For each table column, find if it exists in
				// the input columns
				for (table_idx, table_column) in table.columns.iter().enumerate() {
					let mut value = if let Some(input_column) =
						columns.iter().find(|col| col.name() == table_column.name)
					{
						input_column.data().get_value(row_numberx)
					} else {
						Value::Undefined
					};

					// Handle auto-increment columns
					if table_column.auto_increment && matches!(value, Value::Undefined) {
						value = ColumnSequence::next_value(
							std_txn.command_mut(),
							table.id,
							table_column.id,
						)?;
					}

					// Create ResolvedColumn for this column
					let column_ident = Fragment::owned_internal(table_column.name.clone());
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

					// Validate the value against the
					// column's constraint
					if let Err(e) = table_column.constraint.validate(&value) {
						return Err(e);
					}

					match value {
						Value::Boolean(v) => layout.set_bool(&mut row, table_idx, v),
						Value::Float4(v) => layout.set_f32(&mut row, table_idx, *v),
						Value::Float8(v) => layout.set_f64(&mut row, table_idx, *v),
						Value::Int1(v) => layout.set_i8(&mut row, table_idx, v),
						Value::Int2(v) => layout.set_i16(&mut row, table_idx, v),
						Value::Int4(v) => layout.set_i32(&mut row, table_idx, v),
						Value::Int8(v) => layout.set_i64(&mut row, table_idx, v),
						Value::Int16(v) => layout.set_i128(&mut row, table_idx, v),
						Value::Utf8(v) => layout.set_utf8(&mut row, table_idx, v),
						Value::Uint1(v) => layout.set_u8(&mut row, table_idx, v),
						Value::Uint2(v) => layout.set_u16(&mut row, table_idx, v),
						Value::Uint4(v) => layout.set_u32(&mut row, table_idx, v),
						Value::Uint8(v) => layout.set_u64(&mut row, table_idx, v),
						Value::Uint16(v) => layout.set_u128(&mut row, table_idx, v),
						Value::Date(v) => layout.set_date(&mut row, table_idx, v),
						Value::DateTime(v) => layout.set_datetime(&mut row, table_idx, v),
						Value::Time(v) => layout.set_time(&mut row, table_idx, v),
						Value::Interval(v) => layout.set_interval(&mut row, table_idx, v),
						Value::RowNumber(_v) => {}
						Value::IdentityId(v) => layout.set_identity_id(&mut row, table_idx, v),
						Value::Uuid4(v) => layout.set_uuid4(&mut row, table_idx, v),
						Value::Uuid7(v) => layout.set_uuid7(&mut row, table_idx, v),
						Value::Blob(v) => layout.set_blob(&mut row, table_idx, &v),
						Value::Int(v) => layout.set_int(&mut row, table_idx, &v),
						Value::Uint(v) => layout.set_uint(&mut row, table_idx, &v),
						Value::Decimal(v) => layout.set_decimal(&mut row, table_idx, &v),
						Value::Undefined => layout.set_undefined(&mut row, table_idx),
					}
				}

				// 	// Insert the row into the database
				// 	let row_number =
				// TableRowSequence::next_row_number( 		txn,
				// table.id, 	)?;
				// 	txn.set(
				// 		&TableRowKey {
				// 			table: table.id,
				// 			row: row_number,
				// 		}
				// 		.encode(),
				// 		row.clone(),
				// 	)
				// 	.unwrap();
				//
				// 	// Add to pending changes for flow
				// processing
				// txn.add_pending(Pending::InsertIntoTable {
				// 		table: table.clone(),
				// 		row_number,
				// 		row: row.clone(),
				// 	});
				//
				// txn.insert_into_table(table, key, row)

				let row_number = std_txn.command_mut().insert_into_table(table.clone(), row.clone())?;

				// Store primary key index entry if table has
				// one
				if let Some(pk_def) = primary_key::get_primary_key(std_txn.command_mut(), &table)? {
					let index_key =
						primary_key::encode_primary_key(&pk_def, &row, &table, &layout)?;

					// Store the index entry with the row
					// number as value For now, we
					// encode the row number as a simple
					// EncodedRow with u64
					let row_number_layout = EncodedRowLayout::new(&[Type::Uint8]);
					let mut row_number_encoded = row_number_layout.allocate_row();
					row_number_layout.set_u64(&mut row_number_encoded, 0, u64::from(row_number));

					std_txn.command_mut().set(
						&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key)
							.encode(),
						row_number_encoded,
					)?;
				}

				inserted_count += 1;
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("table", Value::Utf8(table.name)),
			("inserted", Value::Uint8(inserted_count as u64)),
		]))
	}
}
