// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, namespace_not_found},
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		policy::{DataOp, PolicyTargetType},
	},
	internal_error,
	value::{
		batch::batch,
		column::{
			builder::ColumnBuilder,
			cast::{cast_column_data, cast_value, convert::TargetConvert},
			factory,
			write::{check_digest_write, check_digest_write_type},
		},
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::InsertDictionaryNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		column_view::ColumnView,
		dictionary::DictionaryEntryId,
		system_columns::{column_view, user_columns},
		value_type::ValueType,
	},
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
) -> Result<RecordBatch> {
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

		let source_column = match column_view(&columns, "value")? {
			Some(view) => Some(view),
			None => user_columns(&columns)
				.next()
				.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
				.transpose()?,
		};
		if let Some(source) = &source_column {
			values.extend(cast_to_dictionary_type(source, &dictionary.value_type)?);
		}
	}

	let ids = txn.intern_values(&dictionary, values.clone())?;

	if let Some(returning_exprs) = &plan.returning {
		let id_column = build_id_column(&ids, dictionary.id_type)?;
		let value_column = build_value_column(&values, dictionary.value_type);
		let columns = batch(vec![id_column, value_column])?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}

	if ids.is_empty() {
		return batch(vec![
			factory::utf8("namespace", vec![namespace.name()]),
			factory::utf8("dictionary", vec![dictionary.name.clone()]),
			factory::uint8("inserted", vec![0]),
		]);
	}

	let id_column = build_id_column(&ids, dictionary.id_type)?;

	let value_column = build_value_column(&values, dictionary.value_type);

	batch(vec![
		factory::utf8("namespace", vec![namespace.name(); ids.len()]),
		factory::utf8("dictionary", vec![dictionary.name.clone(); ids.len()]),
		id_column,
		value_column,
	])
}

fn cast_to_dictionary_type(source: &ColumnView<'_>, target_type: &ValueType) -> Result<Vec<Value>> {
	let defined: Vec<Value> = source.iter().filter(|value| !matches!(value, Value::None { .. })).collect();
	if defined.is_empty() {
		return Ok(defined);
	}
	let mut compact = ColumnBuilder::with_capacity(source.base_type(), defined.len());
	for value in &defined {
		compact.push_value(value.clone());
	}
	let compact = compact.finish(source.field.name());
	let compact = ColumnView::try_from(&compact)?;
	let convert = TargetConvert {
		target: None,
	};
	let cast = check_digest_write(&compact, target_type, || Fragment::None)
		.and_then(|()| cast_column_data(convert, &compact, target_type.clone(), || Fragment::None));
	match cast {
		Ok(cast) => Ok(ColumnView::try_from(&cast)?.iter().collect()),
		Err(_) => {
			for value in defined {
				coerce_value_to_dictionary_type(value, target_type)?;
			}
			Err(internal_error!("a dictionary column cast failed where every value cast passes"))
		}
	}
}

fn coerce_value_to_dictionary_type(value: Value, target_type: &ValueType) -> Result<Value> {
	let display = value.to_string();
	check_digest_write_type(&value.get_type(), target_type, || Fragment::internal(&display))?;
	cast_value(value, target_type)
}

fn build_id_column(ids: &[DictionaryEntryId], id_type: ValueType) -> Result<(FieldRef, ArrayRef)> {
	let data = match id_type {
		ValueType::Uint1 => {
			let vals: Vec<u8> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U1(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint1("id", vals)
		}
		ValueType::Uint2 => {
			let vals: Vec<u16> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U2(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint2("id", vals)
		}
		ValueType::Uint4 => {
			let vals: Vec<u32> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U4(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint4("id", vals)
		}
		ValueType::Uint8 => {
			let vals: Vec<u64> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U8(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint8("id", vals)
		}
		ValueType::Uint16 => {
			let vals: Vec<u128> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U16(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint16("id", vals)
		}
		_ => {
			let vals: Vec<u64> = ids
				.iter()
				.map(|id| match id {
					DictionaryEntryId::U8(n) => *n,
					_ => 0,
				})
				.collect();
			factory::uint8("id", vals)
		}
	};

	Ok(data)
}

fn build_value_column(values: &[Value], value_type: ValueType) -> (FieldRef, ArrayRef) {
	let mut data = ColumnBuilder::with_capacity(value_type, values.len());
	for value in values {
		data.push_value(value.clone());
	}
	data.finish("value")
}
