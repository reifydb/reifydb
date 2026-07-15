// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound::Included, sync::Arc};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKeyRange};
use reifydb_core::{
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::IndexId,
			key::PrimaryKey,
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			shape::ShapeId,
			table::Table,
		},
		resolved::{ResolvedNamespace, ResolvedShape, ResolvedTable},
	},
	internal_error,
	key::{
		EncodableKey, EncodableKeyRange,
		index_entry::IndexEntryKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::{RowKey, RowKeyRange},
	},
	value::column::columns::Columns,
};
use reifydb_rql::{nodes::DeleteTableNode, query::QueryPlan};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, partition::Partition, row_number::RowNumber},
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
	partition::row_key_from_partition,
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
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
		batch_size: exec.services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity: IdentityId::root(),
		memory: query_budget(&exec.services),
	};
	let mut input_node = compile(input_plan, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let (row_numbers_to_delete, partitions_to_delete) =
		collect_rows_to_delete(exec, txn, &mut input_node, &context, target)?;

	if !target.table.partition_by.is_empty() && partitions_to_delete.len() != row_numbers_to_delete.len() {
		return Err(EngineError::MissingPartitionAddress {
			shape: ShapeId::Table(target.table.id),
			operation: "DELETE",
		}
		.into());
	}

	let pk_def = primary_key::get_primary_key(&exec.services.catalog, txn, target.table)?;

	let mut filtered_ids: Vec<RowNumber> = Vec::with_capacity(row_numbers_to_delete.len());
	let mut filtered_partitions: Vec<Partition> = Vec::with_capacity(partitions_to_delete.len());
	for (idx, row_number) in row_numbers_to_delete.into_iter().enumerate() {
		let partition = partitions_to_delete.get(idx).copied();
		let row_key = row_key_from_partition(target.table.id, partition, row_number);
		let row_values = match txn.get(&row_key)? {
			Some(v) => v.row,
			None => continue,
		};
		if let Some(ref pk_def) = pk_def {
			remove_table_pk_index_for(exec.services, txn, target.table, pk_def, &row_values)?;
		}
		filtered_ids.push(row_number);
		if let Some(p) = partition {
			filtered_partitions.push(p);
		}
	}

	let removed = txn.remove_from_table(target.table, &filtered_ids, &filtered_partitions)?;
	let deleted_count = removed.len() as u64;
	let returned_rows: Vec<(RowNumber, EncodedRow)> = if has_returning {
		removed
	} else {
		Vec::new()
	};
	Ok((deleted_count, returned_rows))
}

fn collect_rows_to_delete(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_node: &mut Box<dyn QueryNode>,
	context: &QueryContext,
	target: &TableTarget<'_>,
) -> Result<(Vec<RowNumber>, Vec<Partition>)> {
	let mut row_numbers_to_delete = Vec::new();
	let mut partitions_to_delete = Vec::new();
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
			if !columns.partitions.is_empty() {
				partitions_to_delete.push(columns.partitions[row_idx]);
			}
		}
	}
	Ok((row_numbers_to_delete, partitions_to_delete))
}

fn run_table_delete_all(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	table: &Table,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedRow)>)> {
	let partitioned = !table.partition_by.is_empty();
	let range = if partitioned {
		PartitionedRowKey::full_scan(table.id)
	} else {
		let range = RowKeyRange {
			shape: table.id.into(),
		};
		EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap()))
	};
	let pk_def = primary_key::get_primary_key(&services.catalog, txn, table)?;
	let rows: Vec<_> = txn.range(range, RangeScope::All, 32)?.collect::<Result<Vec<_>>>()?;

	let mut filtered_ids: Vec<RowNumber> = Vec::with_capacity(rows.len());
	let mut filtered_partitions: Vec<Partition> = Vec::with_capacity(rows.len());
	for multi in rows {
		if let Some(ref pk_def) = pk_def {
			remove_table_pk_index_for(services, txn, table, pk_def, &multi.row)?;
		}
		if partitioned {
			let key = PartitionedRowKey::decode(&multi.key).expect("valid PartitionedRowKey encoding");
			if let RowLocator::Row(rn) = key.locator {
				filtered_ids.push(rn);
				filtered_partitions.push(key.partition);
			}
		} else {
			let row_key = RowKey::decode(&multi.key).expect("valid RowKey encoding");
			filtered_ids.push(row_key.row);
		}
	}

	let removed = txn.remove_from_table(table, &filtered_ids, &filtered_partitions)?;
	let deleted_count = removed.len() as u64;
	let returned_rows: Vec<(RowNumber, EncodedRow)> = if has_returning {
		removed
	} else {
		Vec::new()
	};
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
