// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::Bound::Included, sync::Arc};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{
	encoded::{key::EncodedKeyRange, row::EncodedRow},
	interface::{
		catalog::{
			id::IndexId,
			key::PrimaryKey,
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			table::Table,
		},
		resolved::{ResolvedNamespace, ResolvedShape, ResolvedTable},
	},
	internal_error,
	key::{
		EncodableKey, EncodableKeyRange,
		index_entry::IndexEntryKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::columns::Columns,
};
use reifydb_rql::{nodes::DeleteTableNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use super::{
	context::{TableTarget, WriteExecCtx},
	primary_key,
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	error::EngineError,
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

pub(crate) fn delete(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteTableNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let DeleteTableNode {
		input,
		target,
		returning,
	} = plan;
	let target = target.expect("DELETE without input requires explicit target table");
	let (namespace, table) = resolve_delete_table_target(services, txn, &target)?;
	let resolved_source = build_delete_table_resolved_source(&namespace, &table);
	let target_data = TableTarget {
		namespace: &namespace,
		table: &table,
		fragment: target.identifier(),
	};

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let (deleted_count, returned_rows) = if let Some(input_plan) = input {
		run_table_delete_with_input(
			&exec,
			txn,
			*input_plan,
			&target_data,
			&resolved_source,
			&params,
			returning.is_some(),
		)?
	} else {
		run_table_delete_all(services, txn, &table, returning.is_some())?
	};

	if let Some(returning_exprs) = &returning {
		let shape = get_or_create_table_shape(&services.catalog, &table, txn)?;
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(delete_table_result(namespace.name(), &table.name, deleted_count))
}

#[inline]
fn resolve_delete_table_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedTable,
) -> Result<(Namespace, Table)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: namespace_name.to_string(),
			name: String::new(),
			fragment: Fragment::internal(namespace_name),
		}
		.into());
	};
	let Some(table) = services.catalog.find_table_by_name(txn, namespace.id(), target.name())? else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Table,
			namespace: namespace_name.to_string(),
			name: target.name().to_string(),
			fragment: target.identifier().clone(),
		}
		.into());
	};
	Ok((namespace, table))
}

#[inline]
fn build_delete_table_resolved_source(namespace: &Namespace, table: &Table) -> Option<ResolvedShape> {
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let table_ident = Fragment::internal(table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
	Some(ResolvedShape::Table(resolved_table))
}

fn run_table_delete_with_input(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	target: &TableTarget<'_>,
	resolved_source: &Option<ResolvedShape>,
	params: &Params,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedRow)>)> {
	let context = QueryContext {
		services: exec.services.clone(),
		source: resolved_source.clone(),
		batch_size: 1024,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity: IdentityId::root(),
	};
	let mut input_node = compile(input_plan, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let row_numbers_to_delete = collect_row_numbers_to_delete(exec, txn, &mut input_node, &context, target)?;

	let pk_def = primary_key::get_primary_key(&exec.services.catalog, txn, target.table)?;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let mut deleted_count = 0u64;

	for row_number in row_numbers_to_delete {
		let row_key = RowKey::encoded(target.table.id, row_number);
		let row_values = match txn.get(&row_key)? {
			Some(v) => v.row,
			None => continue,
		};

		if let Some(ref pk_def) = pk_def {
			remove_table_pk_index_for(exec.services, txn, target.table, pk_def, &row_values)?;
		}

		let deleted_values = txn.remove_from_table(target.table.clone(), row_number)?;
		if has_returning {
			returned_rows.push((row_number, deleted_values));
		}
		deleted_count += 1;
	}
	Ok((deleted_count, returned_rows))
}

fn collect_row_numbers_to_delete(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_node: &mut Box<dyn QueryNode>,
	context: &QueryContext,
	target: &TableTarget<'_>,
) -> Result<Vec<RowNumber>> {
	let mut row_numbers_to_delete = Vec::new();
	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.table.name,
			DataOp::Delete,
			&columns,
			PolicyTargetType::Table,
		)?;
		if columns.row_numbers.is_empty() {
			return Err(EngineError::MissingRowNumberColumn.into());
		}
		let row_numbers = &columns.row_numbers;
		for row_idx in 0..columns.row_count() {
			row_numbers_to_delete.push(row_numbers[row_idx]);
		}
	}
	Ok(row_numbers_to_delete)
}

fn run_table_delete_all(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	table: &Table,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedRow)>)> {
	let range = RowKeyRange {
		shape: table.id.into(),
	};
	let pk_def = primary_key::get_primary_key(&services.catalog, txn, table)?;
	let rows: Vec<_> = txn
		.range(EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap())), 1024)?
		.collect::<Result<Vec<_>>>()?;

	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
	let mut deleted_count = 0u64;
	for multi in rows {
		if let Some(ref pk_def) = pk_def {
			remove_table_pk_index_for(services, txn, table, pk_def, &multi.row)?;
		}
		let row_key = RowKey::decode(&multi.key).expect("valid RowKey encoding");
		let deleted_values = txn.remove_from_table(table.clone(), row_key.row)?;
		if has_returning {
			returned_rows.push((row_key.row, deleted_values));
		}
		deleted_count += 1;
	}
	Ok((deleted_count, returned_rows))
}

#[inline]
fn remove_table_pk_index_for(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	table: &Table,
	pk_def: &PrimaryKey,
	row_values: &EncodedRow,
) -> Result<()> {
	let fingerprint = row_values.fingerprint();
	let shape = services.catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
		internal_error!("Shape with fingerprint {:?} not found for table {}", fingerprint, table.name)
	})?;
	let index_key = primary_key::encode_primary_key(pk_def, row_values, table, &shape)?;
	txn.remove(&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key).encode())?;
	Ok(())
}

#[inline]
fn delete_table_result(namespace: &str, table: &str, deleted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}
