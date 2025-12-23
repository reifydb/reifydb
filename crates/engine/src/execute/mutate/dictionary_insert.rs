// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	interface::Params,
	return_error,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_rql::plan::physical::InsertDictionaryNode;
use reifydb_type::{DictionaryEntryId, Fragment, Type, Value, diagnostic::catalog::dictionary_not_found};

use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
	transaction::operation::DictionaryOperations,
};

impl Executor {
	pub(crate) async fn insert_dictionary<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: InsertDictionaryNode,
		stack: &mut Stack,
	) -> crate::Result<Columns> {
		let namespace_name = plan.target.namespace().name();

		let namespace = CatalogStore::find_namespace_by_name(txn, namespace_name).await?.unwrap();

		let dictionary_name = plan.target.name();
		let Some(dictionary) =
			CatalogStore::find_dictionary_by_name(txn, namespace.id, dictionary_name).await?
		else {
			let fragment = plan.target.identifier().clone();
			return_error!(dictionary_not_found(fragment.clone(), namespace_name, dictionary_name,));
		};

		// No resolved source needed for dictionary insert - dictionary has fixed (id, value) schema
		let execution_context = Arc::new(ExecutionContext {
			executor: self.clone(),
			source: None,
			batch_size: 1024,
			params: Params::None,
			stack: stack.clone(),
		});

		let mut std_txn = StandardTransaction::from(txn);
		let mut input_node = compile(*plan.input, &mut std_txn, execution_context.clone()).await;

		// Initialize the operator before execution
		input_node.initialize(&mut std_txn, &execution_context).await?;

		// Collect all inserted (id, value) pairs
		let mut ids: Vec<Value> = Vec::new();
		let mut values: Vec<Value> = Vec::new();
		let mut mutable_context = (*execution_context).clone();

		while let Some(Batch {
			columns,
		}) = input_node.next(&mut std_txn, &mut mutable_context).await?
		{
			let row_count = columns.row_count();

			for row_idx in 0..row_count {
				// Dictionary expects a single value column - find it
				// The input could have columns like "value" or just the first column
				let value = if let Some(value_column) = columns.iter().find(|col| col.name() == "value")
				{
					value_column.data().get_value(row_idx)
				} else if let Some(first_column) = columns.iter().next() {
					// Use first column if no explicit "value" column
					first_column.data().get_value(row_idx)
				} else {
					Value::Undefined
				};

				// Skip undefined values
				if matches!(value, Value::Undefined) {
					continue;
				}

				// Coerce value to dictionary's value_type
				let coerced_value = coerce_value_to_dictionary_type(value, dictionary.value_type)?;

				// Insert into dictionary
				let entry_id = std_txn
					.command_mut()
					.insert_into_dictionary(&dictionary, &coerced_value)
					.await?;

				let id_value = match entry_id {
					DictionaryEntryId::U1(v) => Value::Uint1(v),
					DictionaryEntryId::U2(v) => Value::Uint2(v),
					DictionaryEntryId::U4(v) => Value::Uint4(v),
					DictionaryEntryId::U8(v) => Value::Uint8(v),
					DictionaryEntryId::U16(v) => Value::Uint16(v),
				};

				ids.push(id_value);
				values.push(coerced_value);
			}
		}

		// Return result with inserted entries
		if ids.is_empty() {
			// No entries inserted - return empty result
			return Ok(Columns::new(vec![
				Column {
					name: Fragment::internal("namespace"),
					data: ColumnData::utf8(vec![namespace.name.clone()]),
				},
				Column {
					name: Fragment::internal("dictionary"),
					data: ColumnData::utf8(vec![dictionary.name.clone()]),
				},
				Column {
					name: Fragment::internal("inserted"),
					data: ColumnData::uint8(vec![0]),
				},
			]));
		}

		// Build id column based on dictionary's id_type
		let id_column = build_id_column(&ids, dictionary.id_type)?;

		// Build value column based on dictionary's value_type
		let value_column = build_value_column(&values, dictionary.value_type)?;

		Ok(Columns::new(vec![
			Column {
				name: Fragment::internal("namespace"),
				data: ColumnData::utf8(vec![namespace.name.clone(); ids.len()]),
			},
			Column {
				name: Fragment::internal("dictionary"),
				data: ColumnData::utf8(vec![dictionary.name.clone(); ids.len()]),
			},
			id_column,
			value_column,
		]))
	}
}

/// Coerce a value to the dictionary's value_type
fn coerce_value_to_dictionary_type(value: Value, target_type: Type) -> crate::Result<Value> {
	// Simple coercion - for now just validate type matches or do basic conversions
	match (&value, target_type) {
		// Exact type match
		(Value::Utf8(_), Type::Utf8) => Ok(value),
		(Value::Int1(_), Type::Int1) => Ok(value),
		(Value::Int2(_), Type::Int2) => Ok(value),
		(Value::Int4(_), Type::Int4) => Ok(value),
		(Value::Int8(_), Type::Int8) => Ok(value),
		(Value::Int16(_), Type::Int16) => Ok(value),
		(Value::Uint1(_), Type::Uint1) => Ok(value),
		(Value::Uint2(_), Type::Uint2) => Ok(value),
		(Value::Uint4(_), Type::Uint4) => Ok(value),
		(Value::Uint8(_), Type::Uint8) => Ok(value),
		(Value::Uint16(_), Type::Uint16) => Ok(value),
		(Value::Float4(_), Type::Float4) => Ok(value),
		(Value::Float8(_), Type::Float8) => Ok(value),
		(Value::Boolean(_), Type::Boolean) => Ok(value),
		(Value::Date(_), Type::Date) => Ok(value),
		(Value::DateTime(_), Type::DateTime) => Ok(value),
		(Value::Time(_), Type::Time) => Ok(value),
		(Value::Duration(_), Type::Duration) => Ok(value),
		(Value::Uuid4(_), Type::Uuid4) => Ok(value),
		(Value::Uuid7(_), Type::Uuid7) => Ok(value),
		(Value::Blob(_), Type::Blob) => Ok(value),
		(Value::Decimal(_), Type::Decimal) => Ok(value),
		// TODO: Add more coercion cases as needed
		_ => {
			// For now, return the value as-is and let the storage handle it
			Ok(value)
		}
	}
}

/// Build the ID column based on the dictionary's id_type
fn build_id_column(ids: &[Value], id_type: Type) -> crate::Result<Column> {
	let data = match id_type {
		Type::Uint1 => {
			let vals: Vec<u8> = ids
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = ids
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = ids
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = ids
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint8(vals)
		}
		Type::Uint16 => {
			let vals: Vec<u128> = ids
				.iter()
				.map(|v| match v {
					Value::Uint16(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint16(vals)
		}
		_ => {
			// Fallback to uint8
			let vals: Vec<u64> = ids
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint8(vals)
		}
	};

	Ok(Column {
		name: Fragment::internal("id"),
		data,
	})
}

/// Build the value column based on the dictionary's value_type
fn build_value_column(values: &[Value], value_type: Type) -> crate::Result<Column> {
	let data = match value_type {
		Type::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => format!("{:?}", v),
				})
				.collect();
			ColumnData::utf8(vals)
		}
		Type::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int1(vals)
		}
		Type::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int2(vals)
		}
		Type::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int4(vals)
		}
		Type::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int8(vals)
		}
		Type::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint8(vals)
		}
		_ => {
			// Fallback to string representation
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnData::utf8(vals)
		}
	};

	Ok(Column {
		name: Fragment::internal("value"),
		data,
	})
}
