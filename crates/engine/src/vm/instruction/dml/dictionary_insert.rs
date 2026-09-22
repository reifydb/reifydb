// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, namespace_not_found},
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		policy::{DataOp, PolicyTargetType},
	},
	value::column::{
		ColumnWithName,
		buffer::{ColumnBuffer, write::check_digest_write_type},
		builder::ColumnBuilder,
		cast::cast_value,
		columns::Columns,
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::InsertDictionaryNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, dictionary::DictionaryEntryId, value_type::ValueType},
};

use super::returning::evaluate_returning;
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::dictionary::DictionaryOperations,
	vm::{
		services::Services,
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
		identity: txn.identity(),
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

			let coerced_value = coerce_value_to_dictionary_type(value, &dictionary.value_type)?;

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
		let id_column = build_id_column(&ids, dictionary.id_type)?;
		let value_column = build_value_column(&values, dictionary.value_type);
		let columns = Columns::new(vec![id_column, value_column]);
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
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

	let value_column = build_value_column(&values, dictionary.value_type);

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

fn coerce_value_to_dictionary_type(value: Value, target_type: &ValueType) -> Result<Value> {
	let display = value.to_string();
	check_digest_write_type(&value.get_type(), target_type, || Fragment::internal(&display))?;
	cast_value(value, target_type)
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

fn build_value_column(values: &[Value], value_type: ValueType) -> ColumnWithName {
	let mut data = ColumnBuilder::with_capacity(value_type, values.len());
	for value in values {
		data.push_value(value.clone());
	}
	ColumnWithName {
		name: Fragment::internal("value"),
		data: data.finish(),
	}
}
