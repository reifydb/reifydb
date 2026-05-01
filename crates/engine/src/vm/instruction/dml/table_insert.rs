// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		index::primary_key_violation,
	},
	interface::{
		catalog::{
			id::IndexId,
			key::PrimaryKey,
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			table::Table,
		},
		resolved::{ResolvedColumn, ResolvedNamespace, ResolvedShape, ResolvedTable},
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::InsertTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber, r#type::Type},
};
use tracing::instrument;

use super::{
	context::TableTarget,
	primary_key,
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	transaction::operation::{dictionary::DictionaryOperations, table::TableOperations},
	vm::{
		instruction::dml::coerce::coerce_value_to_column_type,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

#[instrument(name = "mutate::table::insert", level = "trace", skip_all)]
pub(crate) fn insert_table(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertTableNode,
	symbols: &mut SymbolTable,
) -> Result<Columns> {
	let InsertTableNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, table) = resolve_insert_table_target(services, txn, &target)?;
	let shape = get_or_create_table_shape(&services.catalog, &table, txn)?;
	let target_data = TableTarget {
		namespace: &namespace,
		table: &table,
		fragment: target.identifier(),
	};
	let context = build_insert_table_query_context(services, &target_data, symbols);
	let mut input_node = compile(*input, txn, context.clone());
	input_node.initialize(txn, &context)?;

	let validated_rows = validate_and_encode_input_rows(
		services,
		txn,
		&target_data,
		&shape,
		&context,
		symbols,
		&mut input_node,
	)?;

	let total_rows = validated_rows.len();
	if total_rows == 0 {
		return Ok(insert_table_result(namespace.name(), &table.name, 0));
	}

	let row_numbers = services.catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;
	assert_eq!(row_numbers.len(), validated_rows.len());

	let pk_def = primary_key::get_primary_key(&services.catalog, txn, &table)?;
	let row_number_shape = pk_def.as_ref().map(|_| RowShape::testing(&[Type::Uint8]));
	let pk_ctx = pk_def.as_ref().map(|pk| PkContext {
		pk_def: pk,
		row_number_shape: row_number_shape.as_ref().unwrap(),
	});
	let returned_rows = insert_validated_table_rows(
		txn,
		&target_data,
		&shape,
		&validated_rows,
		&row_numbers,
		returning.is_some(),
		pk_ctx.as_ref(),
	)?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(insert_table_result(namespace.name(), &table.name, total_rows as u64))
}

/// Primary key + its row-number shape - always travel together when set.
struct PkContext<'a> {
	pk_def: &'a PrimaryKey,
	row_number_shape: &'a RowShape,
}

/// Input columns + an index over them by name.
struct ColumnView<'a> {
	columns: &'a Columns,
	column_map: &'a HashMap<&'a str, usize>,
}

#[inline]
fn resolve_insert_table_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedTable,
) -> Result<(Namespace, Table)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let table_name = target.name();
	let Some(table) = services.catalog.find_table_by_name(txn, namespace.id(), table_name)? else {
		let fragment = target.identifier().clone();
		return_error!(table_not_found(fragment.clone(), namespace_name, table_name,));
	};
	Ok((namespace, table))
}

#[inline]
fn build_insert_table_query_context(
	services: &Arc<Services>,
	target: &TableTarget<'_>,
	symbols: &SymbolTable,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let table_ident = Fragment::internal(target.table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, target.table.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::Table(resolved_table)),
		batch_size: 32,
		params: Params::None,
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	})
}

fn validate_and_encode_input_rows(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	context: &Arc<QueryContext>,
	symbols: &SymbolTable,
	input_node: &mut Box<dyn QueryNode>,
) -> Result<Vec<EncodedRow>> {
	let mut validated_rows: Vec<EncodedRow> = Vec::new();
	let mut mutable_context = (**context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.table.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Table,
		)?;
		let mut column_map: HashMap<&str, usize> = HashMap::new();
		for (idx, col) in columns.iter().enumerate() {
			column_map.insert(col.name().text(), idx);
		}
		let view = ColumnView {
			columns: &columns,
			column_map: &column_map,
		};
		let row_count = columns.row_count();
		for row_idx in 0..row_count {
			validated_rows
				.push(build_insert_table_row(services, txn, target, shape, &view, context, row_idx)?);
		}
	}
	Ok(validated_rows)
}

#[inline]
fn build_insert_table_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	view: &ColumnView<'_>,
	context: &Arc<QueryContext>,
	row_idx: usize,
) -> Result<EncodedRow> {
	let mut row = shape.allocate();
	for (table_idx, table_column) in target.table.columns.iter().enumerate() {
		let mut value = if let Some(&input_idx) = view.column_map.get(table_column.name.as_str()) {
			view.columns[input_idx].get_value(row_idx)
		} else {
			Value::none()
		};
		if table_column.auto_increment && matches!(value, Value::None { .. }) {
			value = services.catalog.column_sequence_next_value(txn, target.table.id, table_column.id)?;
		}
		let column_ident = view
			.column_map
			.get(table_column.name.as_str())
			.map(|&idx| view.columns.name_at(idx).clone())
			.unwrap_or_else(|| Fragment::internal(table_column.name.clone()));
		let resolved_column = ResolvedColumn::new(
			column_ident.clone(),
			context.source.clone().unwrap(),
			table_column.clone(),
		);
		value = coerce_value_to_column_type(
			value,
			table_column.constraint.get_type(),
			resolved_column,
			context,
		)?;
		if let Err(mut e) = table_column.constraint.validate(&value) {
			e.0.fragment = column_ident.clone();
			return Err(e);
		}
		let value = if let Some(dict_id) = table_column.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, table_column.name)
			})?;
			let entry_id = txn.insert_into_dictionary(&dictionary, &value)?;
			entry_id.to_value()
		} else {
			value
		};
		shape.set_value(&mut row, table_idx, &value);
	}
	let now_nanos = services.runtime_context.clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	Ok(row)
}

fn insert_validated_table_rows(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	validated_rows: &[EncodedRow],
	row_numbers: &[RowNumber],
	has_returning: bool,
	pk: Option<&PkContext<'_>>,
) -> Result<Vec<(RowNumber, EncodedRow)>> {
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = if has_returning {
		Vec::with_capacity(validated_rows.len())
	} else {
		Vec::new()
	};

	for (row, &row_number) in validated_rows.iter().zip(row_numbers.iter()) {
		let stored_row = txn.insert_table(target.table, shape, row.clone(), row_number)?;
		if has_returning {
			returned_rows.push((row_number, stored_row));
		}
		if let Some(pk) = pk {
			write_insert_table_pk_index(txn, target, shape, pk, row, row_number)?;
		}
	}
	Ok(returned_rows)
}

#[inline]
fn write_insert_table_pk_index(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	pk: &PkContext<'_>,
	row: &EncodedRow,
	row_number: RowNumber,
) -> Result<()> {
	let index_key = primary_key::encode_primary_key(pk.pk_def, row, target.table, shape)?;
	let index_entry_key = IndexEntryKey::new(target.table.id, IndexId::primary(pk.pk_def.id), index_key.clone());
	if txn.contains_key(&index_entry_key.encode())? {
		let key_columns = pk.pk_def.columns.iter().map(|c| c.name.clone()).collect();
		return_error!(primary_key_violation(target.fragment.clone(), target.table.name.clone(), key_columns,));
	}
	let mut row_number_encoded = pk.row_number_shape.allocate();
	pk.row_number_shape.set_u64(&mut row_number_encoded, 0, u64::from(row_number));
	txn.set(&index_entry_key.encode(), row_number_encoded)?;
	Ok(())
}

#[inline]
fn insert_table_result(namespace: &str, table: &str, inserted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}
