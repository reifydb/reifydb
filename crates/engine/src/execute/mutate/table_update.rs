// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::{
		CommandTransaction, EncodableKey, IndexEntryKey, IndexId, Params, QueryTransaction, ResolvedColumn,
		ResolvedNamespace, ResolvedPrimitive, ResolvedTable, RowKey,
	},
	value::{column::Columns, encoded::EncodedValuesLayout},
};
use reifydb_rql::plan::physical::UpdateTableNode;
use reifydb_type::{
	Fragment, Type, Value,
	diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
	},
	internal_error, return_error,
};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{
		Batch, ExecutionContext, Executor, QueryNode, mutate::coerce::coerce_value_to_column_type,
		query::compile::compile,
	},
	stack::Stack,
	transaction::operation::DictionaryOperations,
};

impl Executor {
	pub(crate) async fn update_table<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: UpdateTableNode,
		params: Params,
	) -> crate::Result<Columns> {
		// Get table from plan or infer from input pipeline
		let (namespace, table) = if let Some(target) = &plan.target {
			// Namespace and table explicitly specified
			let namespace_name = target.namespace().name();
			let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name).await? else {
				return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
			};

			let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, target.name()).await?
			else {
				let fragment = target.identifier().clone();
				return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
			};

			(namespace, table)
		} else {
			unimplemented!("Cannot infer target table from pipeline - no table found")
		};

		// Build storage layout types - use dictionary ID type for dictionary-encoded columns
		let mut table_types: Vec<Type> = Vec::new();
		for c in &table.columns {
			if let Some(dict_id) = c.dictionary_id {
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
			let mut input_node = compile(*plan.input, &mut wrapped_txn, Arc::new(context.clone())).await;

			input_node.initialize(&mut wrapped_txn, &context).await?;

			let mut mutable_context = context.clone();
			while let Some(Batch {
				columns,
			}) = input_node.next(&mut wrapped_txn, &mut mutable_context).await?
			{
				if columns.row_numbers.is_empty() {
					return_error!(engine::missing_row_number_column());
				}

				let row_numbers = &columns.row_numbers;

				let row_count = columns.row_count();

				for row_numberx in 0..row_count {
					let mut row = layout.allocate();

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
							let dictionary = CatalogStore::find_dictionary(
								wrapped_txn.command_mut(),
								dict_id,
							)
							.await?
							.ok_or_else(|| {
								internal_error!(
									"Dictionary {:?} not found for column {}",
									dict_id,
									table_column.name
								)
							})?;
							let entry_id = wrapped_txn
								.command_mut()
								.insert_into_dictionary(&dictionary, &value)
								.await?;
							entry_id.to_value()
						} else {
							value
						};

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
							Value::Duration(v) => {
								layout.set_duration(&mut row, table_idx, v)
							}
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
							Value::Any(_) => {
								unreachable!("Any type cannot be stored in table")
							}
						}
					}

					let row_number = row_numbers[row_numberx];

					let row_key = RowKey {
						primitive: table.id.into(),
						row: row_number,
					}
					.encode();

					if let Some(pk_def) =
						primary_key::get_primary_key(wrapped_txn.command_mut(), &table).await?
					{
						if let Some(old_row_data) =
							wrapped_txn.command_mut().get(&row_key).await?
						{
							let old_row = old_row_data.values;
							let old_key = primary_key::encode_primary_key(
								&pk_def, &old_row, &table, &layout,
							)?;

							wrapped_txn
								.command_mut()
								.remove(&IndexEntryKey::new(
									table.id,
									IndexId::primary(pk_def.id),
									old_key,
								)
								.encode())
								.await?;
						}

						let new_key = primary_key::encode_primary_key(
							&pk_def, &row, &table, &layout,
						)?;

						let row_number_layout = EncodedValuesLayout::new(&[Type::Uint8]);
						let mut row_number_encoded = row_number_layout.allocate();
						row_number_layout.set_u64(
							&mut row_number_encoded,
							0,
							u64::from(row_number),
						);

						wrapped_txn
							.command_mut()
							.set(
								&IndexEntryKey::new(
									table.id,
									IndexId::primary(pk_def.id),
									new_key,
								)
								.encode(),
								row_number_encoded,
							)
							.await?;
					}

					wrapped_txn.command_mut().set(&row_key, row).await?;

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
