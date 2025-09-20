// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	ColumnDescriptor,
	interface::{
		ColumnPolicyKind, EncodableKey, IndexEntryKey, IndexId, Params, RowKey, Transaction,
		VersionedCommandTransaction, VersionedQueryTransaction,
	},
	row::EncodedRowLayout,
	value::columnar::{ColumnData, Columns},
};
use reifydb_rql::plan::physical::UpdateTableNode;
use reifydb_type::{
	Fragment, ROW_NUMBER_COLUMN_NAME, Type, Value,
	diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
	},
	return_error,
};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{
		Batch, ExecutionContext, Executor, QueryNode, mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
};

impl Executor {
	pub(crate) fn update_table<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: UpdateTableNode<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		// Get table from plan or infer from input pipeline
		let (namespace, table) = if let Some(target) = &plan.target {
			// Namespace and table explicitly specified
			let namespace_name = target.namespace().name();
			let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name)? else {
				return_error!(namespace_not_found(
					Some(target.identifier().namespace.clone().into_owned()),
					namespace_name
				));
			};

			let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, target.name())? else {
				let fragment = target.identifier().name.clone();
				return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
			};

			(namespace, table)
		} else {
			unimplemented!("Cannot infer target table from pipeline - no table found")
		};

		let table_types: Vec<Type> = table.columns.iter().map(|c| c.constraint.get_type()).collect();
		let layout = EncodedRowLayout::new(&table_types);

		// Create execution context
		let context = ExecutionContext {
			functions: self.functions.clone(),
			source: Some(table.clone()),
			batch_size: 1024,
			preserve_row_numbers: true,
			params: params.clone(),
		};

		let mut updated_count = 0;

		// Process all input batches - we need to handle compilation and
		// execution with proper transaction borrowing
		{
			let mut wrapped_txn = StandardTransaction::from(txn);
			let mut input_node = compile(*plan.input, &mut wrapped_txn, Arc::new(context.clone()));

			// Initialize the node before execution
			input_node.initialize(&mut wrapped_txn, &context)?;

			while let Some(Batch {
				columns,
			}) = input_node.next(&mut wrapped_txn)?
			{
				// Find the RowNumber column - return error if
				// not found
				let Some(row_number_column) =
					columns.iter().find(|col| col.name() == ROW_NUMBER_COLUMN_NAME)
				else {
					return_error!(engine::missing_row_number_column());
				};

				// Extract RowNumber data - panic if any are
				// undefined
				let row_numbers = match &row_number_column.data() {
					ColumnData::RowNumber(container) => {
						// Check that all row IDs are
						// defined
						for i in 0..container.data().len() {
							if !container.is_defined(i) {
								return_error!(engine::invalid_row_number_values());
							}
						}
						container.data()
					}
					_ => return_error!(engine::invalid_row_number_values()),
				};

				let row_count = columns.row_count();

				for row_numberx in 0..row_count {
					let mut row = layout.allocate_row();

					// For each table column, find if it
					// exists in the input columns
					for (table_idx, table_column) in table.columns.iter().enumerate() {
						let mut value = if let Some(input_column) =
							columns.iter().find(|col| col.name() == table_column.name)
						{
							input_column.data().get_value(row_numberx)
						} else {
							Value::Undefined
						};

						// Apply automatic type coercion
						// Extract policies (no
						// conversion needed since
						// types are now unified)
						let policies: Vec<ColumnPolicyKind> = table_column
							.policies
							.iter()
							.map(|cp| cp.policy.clone())
							.collect();

						value = coerce_value_to_column_type(
							value,
							table_column.constraint.get_type(),
							ColumnDescriptor::new()
								.with_namespace(Fragment::borrowed_internal(
									&namespace.name,
								))
								.with_table(Fragment::borrowed_internal(&table.name))
								.with_column(Fragment::borrowed_internal(
									&table_column.name,
								))
								.with_column_type(table_column.constraint.get_type())
								.with_policies(policies),
							&context,
						)?;

						// Validate the value against
						// the column's constraint
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
							Value::DateTime(v) => {
								layout.set_datetime(&mut row, table_idx, v)
							}
							Value::Time(v) => layout.set_time(&mut row, table_idx, v),
							Value::Interval(v) => {
								layout.set_interval(&mut row, table_idx, v)
							}
							Value::RowNumber(_v) => {}
							Value::IdentityId(v) => {
								layout.set_identity_id(&mut row, table_idx, v)
							}
							Value::Uuid4(v) => layout.set_uuid4(&mut row, table_idx, v),
							Value::Uuid7(v) => layout.set_uuid7(&mut row, table_idx, v),
							Value::Blob(v) => layout.set_blob(&mut row, table_idx, &v),
							Value::Int(v) => layout.set_int(&mut row, table_idx, &v),
							Value::Uint(v) => layout.set_uint(&mut row, table_idx, &v),
							Value::Decimal(v) => {
								layout.set_decimal(&mut row, table_idx, &v)
							}
							Value::Undefined => layout.set_undefined(&mut row, table_idx),
						}
					}

					// Update the row using the existing
					// RowNumber from the columns
					let row_number = row_numbers[row_numberx];

					let row_key = RowKey {
						source: table.id.into(),
						row: row_number,
					}
					.encode();

					// Handle primary key index if table has
					// one
					if let Some(pk_def) =
						primary_key::get_primary_key(wrapped_txn.command_mut(), &table)?
					{
						// Get old row to extract old PK
						// values
						if let Some(old_row_data) = wrapped_txn.command_mut().get(&row_key)? {
							let old_row = old_row_data.row;
							let old_key = primary_key::encode_primary_key(
								&pk_def, &old_row, &table, &layout,
							)?;

							// Remove old index
							// entry
							wrapped_txn.command_mut().remove(&IndexEntryKey::new(
								table.id,
								IndexId::primary(pk_def.id),
								old_key,
							)
							.encode())?;
						}

						// Add new index entry
						let new_key = primary_key::encode_primary_key(
							&pk_def, &row, &table, &layout,
						)?;

						// Store the row number as value
						let row_number_layout = EncodedRowLayout::new(&[Type::Uint8]);
						let mut row_number_encoded = row_number_layout.allocate_row();
						row_number_layout.set_u64(
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

					// Now update the row
					wrapped_txn.command_mut().set(&row_key, row)?;

					updated_count += 1;
				}
			}
		}

		// Return summary columns
		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
			("table", Value::Utf8(table.name)),
			("updated", Value::Uint8(updated_count as u64)),
		]))
	}
}
