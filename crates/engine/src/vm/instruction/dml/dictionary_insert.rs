// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, namespace_not_found},
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		policy::{DataOp, PolicyTargetType},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::nodes::InsertDictionaryNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, value_type::ValueType},
};

use super::returning::evaluate_returning;
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::dictionary::DictionaryOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

pub(crate) fn insert_dictionary(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertDictionaryNode,
	symbols: &mut SymbolTable,
) -> Result<Columns> {
	let namespace_name = plan.target.namespace().name();

	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let dictionary_name = plan.target.name();
	let Some(dictionary) = services.catalog.find_dictionary_by_name(txn, namespace.id(), dictionary_name)? else {
		let fragment = plan.target.identifier().clone();
		return_error!(dictionary_not_found(fragment.clone(), namespace_name, dictionary_name,));
	};

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: Params::None,
		symbols: symbols.clone(),
		identity: IdentityId::root(),
		memory: query_budget(services),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	input_node.initialize(txn, &execution_context)?;

	let mut ids: Vec<Value> = Vec::new();
	let mut values: Vec<Value> = Vec::new();
	let mut mutable_context = (*execution_context).clone();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace_name,
			dictionary_name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Dictionary,
		)?;

		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			let value = if let Some(value_column) = columns.iter().find(|col| col.name() == "value") {
				value_column.data().get_value(row_idx)
			} else if let Some(first_column) = columns.iter().next() {
				first_column.data().get_value(row_idx)
			} else {
				Value::none()
			};

			if matches!(value, Value::None { .. }) {
				continue;
			}

			let coerced_value = coerce_value_to_dictionary_type(value, dictionary.value_type.clone())?;

			let entry_id = txn.insert_into_dictionary(&dictionary, &coerced_value)?;

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

	if let Some(returning_exprs) = &plan.returning {
		if ids.is_empty() {
			return evaluate_returning(services, symbols, returning_exprs, Columns::empty());
		}
		let id_column = build_id_column(&ids, dictionary.id_type)?;
		let value_column = build_value_column(&values, dictionary.value_type)?;
		let columns = Columns::new(vec![id_column, value_column]);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}

	if ids.is_empty() {
		return Ok(Columns::new(vec![
			ColumnWithName::new(
				Fragment::internal("namespace"),
				ColumnBuffer::utf8(vec![namespace.name()]),
			),
			ColumnWithName::new(
				Fragment::internal("dictionary"),
				ColumnBuffer::utf8(vec![dictionary.name.clone()]),
			),
			ColumnWithName::new(Fragment::internal("inserted"), ColumnBuffer::uint8(vec![0])),
		]));
	}

	let id_column = build_id_column(&ids, dictionary.id_type)?;

	let value_column = build_value_column(&values, dictionary.value_type)?;

	Ok(Columns::new(vec![
		ColumnWithName::new(
			Fragment::internal("namespace"),
			ColumnBuffer::utf8(vec![namespace.name(); ids.len()]),
		),
		ColumnWithName::new(
			Fragment::internal("dictionary"),
			ColumnBuffer::utf8(vec![dictionary.name.clone(); ids.len()]),
		),
		id_column,
		value_column,
	]))
}

fn coerce_value_to_dictionary_type(value: Value, target_type: ValueType) -> Result<Value> {
	match (&value, target_type) {
		(Value::Utf8(_), ValueType::Utf8) => Ok(value),
		(Value::Int1(_), ValueType::Int1) => Ok(value),
		(Value::Int2(_), ValueType::Int2) => Ok(value),
		(Value::Int4(_), ValueType::Int4) => Ok(value),
		(Value::Int8(_), ValueType::Int8) => Ok(value),
		(Value::Int16(_), ValueType::Int16) => Ok(value),
		(Value::Uint1(_), ValueType::Uint1) => Ok(value),
		(Value::Uint2(_), ValueType::Uint2) => Ok(value),
		(Value::Uint4(_), ValueType::Uint4) => Ok(value),
		(Value::Uint8(_), ValueType::Uint8) => Ok(value),
		(Value::Uint16(_), ValueType::Uint16) => Ok(value),
		(Value::Float4(_), ValueType::Float4) => Ok(value),
		(Value::Float8(_), ValueType::Float8) => Ok(value),
		(Value::Boolean(_), ValueType::Boolean) => Ok(value),
		(Value::Date(_), ValueType::Date) => Ok(value),
		(Value::DateTime(_), ValueType::DateTime) => Ok(value),
		(Value::Time(_), ValueType::Time) => Ok(value),
		(Value::Duration(_), ValueType::Duration) => Ok(value),
		(Value::Uuid4(_), ValueType::Uuid4) => Ok(value),
		(Value::Uuid7(_), ValueType::Uuid7) => Ok(value),
		(Value::Blob(_), ValueType::Blob) => Ok(value),
		(Value::Decimal(_), ValueType::Decimal) => Ok(value),
		// TODO: Add more coercion cases as needed
		_ => Ok(value),
	}
}

fn build_id_column(ids: &[Value], id_type: ValueType) -> Result<ColumnWithName> {
	let data = match id_type {
		ValueType::Uint1 => {
			let vals: Vec<u8> = ids
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint1(vals)
		}
		ValueType::Uint2 => {
			let vals: Vec<u16> = ids
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint2(vals)
		}
		ValueType::Uint4 => {
			let vals: Vec<u32> = ids
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint4(vals)
		}
		ValueType::Uint8 => {
			let vals: Vec<u64> = ids
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
		ValueType::Uint16 => {
			let vals: Vec<u128> = ids
				.iter()
				.map(|v| match v {
					Value::Uint16(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint16(vals)
		}
		_ => {
			let vals: Vec<u64> = ids
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
	};

	Ok(ColumnWithName {
		name: Fragment::internal("id"),
		data,
	})
}

fn build_value_column(values: &[Value], value_type: ValueType) -> Result<ColumnWithName> {
	let data = match value_type {
		ValueType::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => format!("{:?}", v),
				})
				.collect();
			ColumnBuffer::utf8(vals)
		}
		ValueType::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int1(vals)
		}
		ValueType::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int2(vals)
		}
		ValueType::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int4(vals)
		}
		ValueType::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int8(vals)
		}
		ValueType::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint1(vals)
		}
		ValueType::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint2(vals)
		}
		ValueType::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint4(vals)
		}
		ValueType::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
		_ => {
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnBuffer::utf8(vals)
		}
	};

	Ok(ColumnWithName {
		name: Fragment::internal("value"),
		data,
	})
}
