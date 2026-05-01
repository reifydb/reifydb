// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
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
	key::{EncodableKey, index_entry::IndexEntryKey, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::UpdateTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use super::{
	context::{TableTarget, WriteExecCtx},
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

pub(crate) fn update_table(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateTableNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let UpdateTableNode {
		input,
		target,
		returning,
	} = plan;
	let target = target.expect("Cannot infer target table from pipeline - no table found");
	let (namespace, table) = resolve_update_table_target(services, txn, &target)?;
	let shape = get_or_create_table_shape(&services.catalog, &table, txn)?;
	let target_data = TableTarget {
		namespace: &namespace,
		table: &table,
		fragment: target.identifier(),
	};
	let context = build_update_table_query_context(services, &target_data, &params, symbols);

	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let (updated_count, returned_rows) =
		run_table_update(&exec, txn, &mut input_node, &target_data, &shape, &context, returning.is_some())?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(update_table_result(namespace.name(), &table.name, updated_count))
}

#[inline]
fn resolve_update_table_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedTable,
) -> Result<(Namespace, Table)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let Some(table) = services.catalog.find_table_by_name(txn, namespace.id(), target.name())? else {
		let fragment = target.identifier().clone();
		return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
	};
	Ok((namespace, table))
}

#[inline]
fn build_update_table_query_context(
	services: &Arc<Services>,
	target: &TableTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let table_ident = Fragment::internal(target.table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, target.table.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::Table(resolved_table)),
		batch_size: 32,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	}
}

fn run_table_update(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_node: &mut Box<dyn QueryNode>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	context: &QueryContext,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedRow)>)> {
	let mut updated_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let mut mutable_context = context.clone();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.table.name,
			DataOp::Update,
			&columns,
			PolicyTargetType::Table,
		)?;

		if columns.row_numbers.is_empty() {
			return_error!(engine::missing_row_number_column());
		}

		let row_numbers = columns.row_numbers.clone();
		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			let mut row = build_updated_table_row(
				exec.services,
				txn,
				target.table,
				shape,
				&columns,
				context,
				row_idx,
			)?;
			let row_number = row_numbers[row_idx];
			let row_key = RowKey::encoded(target.table.id, row_number);

			if let Some(pk_def) = primary_key::get_primary_key(&exec.services.catalog, txn, target.table)? {
				rotate_table_pk_index(txn, target.table, shape, &pk_def, &row_key, &row, row_number)?;
			}

			let old_created_at =
				txn.get(&row_key)?.expect("row must exist for update").row.created_at_nanos();
			row.set_timestamps(old_created_at, exec.services.runtime_context.clock.now_nanos());

			let stored_row = txn.update_table(target.table.clone(), row_number, row)?;
			if has_returning {
				returned_rows.push((row_number, stored_row));
			}
			updated_count += 1;
		}
	}
	Ok((updated_count, returned_rows))
}

#[inline]
fn build_updated_table_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	table: &Table,
	shape: &RowShape,
	columns: &Columns,
	context: &QueryContext,
	row_idx: usize,
) -> Result<EncodedRow> {
	let mut row = shape.allocate();
	for (table_idx, table_column) in table.columns.iter().enumerate() {
		let mut value = if let Some(input_column) = columns.iter().find(|col| col.name() == table_column.name) {
			input_column.data().get_value(row_idx)
		} else {
			Value::none()
		};

		let column_ident = columns
			.iter()
			.find(|col| col.name() == table_column.name)
			.map(|col| col.name().clone())
			.unwrap_or_else(|| Fragment::internal(&table_column.name));
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
	Ok(row)
}

#[inline]
fn rotate_table_pk_index(
	txn: &mut Transaction<'_>,
	table: &Table,
	shape: &RowShape,
	pk_def: &PrimaryKey,
	row_key: &EncodedKey,
	new_row: &EncodedRow,
	row_number: RowNumber,
) -> Result<()> {
	if let Some(pre_row_data) = txn.get(row_key)? {
		let pre_row = pre_row_data.row;
		let pre_key = primary_key::encode_primary_key(pk_def, &pre_row, table, shape)?;
		txn.remove(&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), pre_key).encode())?;
	}

	let post_key = primary_key::encode_primary_key(pk_def, new_row, table, shape)?;
	let row_number_shape = RowShape::testing(&[Type::Uint8]);
	let mut row_number_encoded = row_number_shape.allocate();
	row_number_shape.set_u64(&mut row_number_encoded, 0, u64::from(row_number));
	txn.set(&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), post_key).encode(), row_number_encoded)?;
	Ok(())
}

#[inline]
fn update_table_result(namespace: &str, table: &str, updated: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
