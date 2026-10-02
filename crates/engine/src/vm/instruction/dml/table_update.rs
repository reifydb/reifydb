// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::EncodedBytes,
	pod::EncodedPodRow,
	shape::RowShape,
	table::{EncodedTableRow, EncodedTableRowBuilder},
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
		query::column_not_found,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::IndexId,
			key::PrimaryKey,
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			table::Table,
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedTable},
	},
	key::catalog::IndexEntryKey,
	partition::PartitionError,
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::UpdateTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		identity::IdentityId,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{partitions, row_numbers, user_columns},
	},
};

use super::{
	columns::{ColumnPipeline, input_views, intern_dictionary_columns},
	context::{TableTarget, WriteExecCtx},
	primary_key::{self, PrimaryKeyEncoder},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	error::EngineError,
	partition::{row_key_from_partition, table_partition_of_row},
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		instruction::dml::{coerce::InputFragments, time::resolve_time_for_update},
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

pub(crate) fn update_table(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateTableNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<RecordBatch> {
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
	let context = build_update_table_query_context(services, &target_data, &params, symbols, txn.identity());

	let fragments = InputFragments::of(&input);
	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let (updated_count, returned_rows, pre_rows) = run_table_update(
		&exec,
		txn,
		&mut input_node,
		&fragments,
		&target_data,
		&shape,
		&context,
		returning.is_some(),
	)?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &table.columns, columns)?;
		let pre_columns = decode_rows_to_columns(&shape, &pre_rows)?;
		let pre_columns = decode_returning_dictionaries(services, txn, &table.columns, pre_columns)?;
		let columns = with_pre_image(columns, &pre_columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	update_table_result(namespace.name(), &table.name, updated_count)
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
	identity: IdentityId,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let table_ident = Fragment::internal(target.table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, target.table.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Table(resolved_table)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	}
}

type ReturnedRows = Vec<(RowNumber, EncodedBytes)>;

#[allow(clippy::too_many_arguments)]
fn run_table_update(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_node: &mut Box<dyn QueryNode>,
	fragments: &InputFragments,
	target: &TableTarget<'_>,
	shape: &RowShape,
	context: &QueryContext,
	has_returning: bool,
) -> Result<(u64, ReturnedRows, ReturnedRows)> {
	let mut updated_count = 0u64;
	let mut returned_rows: ReturnedRows = Vec::new();
	let mut pre_rows: ReturnedRows = Vec::new();
	let mut mutable_context = context.clone();
	let pipeline = ColumnPipeline {
		columns: &target.table.columns,
		sequences: None,
		fragments,
		context,
	};
	let pk_def = primary_key::get_primary_key(&exec.services.catalog, txn, target.table)?;
	let mut encoder: Option<PrimaryKeyEncoder> = None;

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		if columns.num_rows() == 0 {
			continue;
		}
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&target.table.name,
			DataOp::Update,
			&columns,
			PolicyTargetType::Table,
		)?;
		if let Some((unknown, _)) = user_columns(&columns)
			.find(|(field, _)| !target.table.columns.iter().any(|c| &c.name == field.name()))
		{
			return_error!(column_not_found(fragments.column(unknown.name())));
		}

		if row_numbers(&columns)?.is_empty() {
			return_error!(engine::missing_row_number_column());
		}

		let partitioned = !target.table.partition_by.is_empty();
		if partitioned && partitions(&columns)?.len() != columns.num_rows() {
			return Err(EngineError::MissingPartitionAddress {
				object: ObjectId::Table(target.table.id),
				operation: "UPDATE",
			}
			.into());
		}

		let row_numbers: Vec<RowNumber> = row_numbers(&columns)?.to_vec();
		let sidecar_partitions: Vec<Partition> = partitions(&columns)?;
		let row_count = columns.num_rows();
		let mut old_rows: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		for (row_idx, &row_number) in row_numbers.iter().enumerate() {
			let row_key = row_key_from_partition(
				target.table.id,
				sidecar_partitions.get(row_idx).copied(),
				row_number,
			);
			old_rows.push(txn.get(&row_key)?.expect("bytes must exist for update").bytes);
		}
		enforce_old_row_policies(exec, txn, target, shape, &row_numbers, &old_rows)?;

		let inputs = input_views(&columns, &target.table.columns)?;
		let mut batches = [pipeline.cast_target_columns(&inputs, row_count, None)?];
		intern_dictionary_columns(exec.services, txn, &target.table.columns, &mut batches)?;
		let mut rows: Vec<EncodedTableRowBuilder> = (0..row_count).map(|_| shape.allocate_table()).collect();
		batches[0].write(shape, &mut rows)?;

		let mut prepared_rows: Vec<EncodedTableRowBuilder> = Vec::with_capacity(row_count);
		let mut partitions_out: Vec<Partition> = Vec::with_capacity(row_count);
		let mut pre_by_row: HashMap<RowNumber, EncodedBytes> = HashMap::new();
		for (row_idx, (mut row, &row_number)) in rows.into_iter().zip(row_numbers.iter()).enumerate() {
			let partition = sidecar_partitions.get(row_idx).copied();

			if let Some(old) = partition {
				let new_partition = table_partition_of_row(target.table, shape, &row);
				if new_partition != old {
					return Err(PartitionError::ImmutablePartitionColumn {
						object: ObjectId::Table(target.table.id),
					}
					.into());
				}
			}

			let old_row = &old_rows[row_idx];
			if let Some(pk_def) = &pk_def {
				let encoder = match &mut encoder {
					Some(encoder) => encoder,
					None => encoder.insert(PrimaryKeyEncoder::new(pk_def, target.table)?),
				};
				rotate_table_pk_index(
					txn,
					target.table,
					shape,
					pk_def,
					encoder,
					old_row,
					&row,
					row_number,
				)?;
			}

			if has_returning {
				pre_by_row.insert(row_number, old_row.clone());
			}
			let old_row = EncodedTableRow::view(old_row);
			let old_created_at = old_row.created_at();
			let old_time = old_row.time();
			let now = exec.services.runtime_context.clock.now();
			row.set_timestamps(old_created_at, now);
			if let Some(time) = resolve_time_for_update(
				&target.table.name,
				&target.table.columns,
				&target.table.time,
				shape,
				&row,
				old_time,
			)? {
				row.set_time(time);
			}

			prepared_rows.push(row);
			if let Some(p) = partition {
				partitions_out.push(p);
			}
		}

		let stored = txn.update_table(target.table, &row_numbers, &partitions_out, &mut prepared_rows)?;
		updated_count += stored.len() as u64;
		if has_returning {
			for (row_number, _) in &stored {
				let pre = pre_by_row
					.remove(row_number)
					.expect("every stored row must carry the pre image read before its write");
				pre_rows.push((*row_number, pre));
			}
			returned_rows.extend(stored);
		}
	}
	Ok((updated_count, returned_rows, pre_rows))
}

fn enforce_old_row_policies(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	row_numbers: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<()> {
	if txn.identity().is_privileged() {
		return Ok(());
	}
	let old_rows: ReturnedRows = row_numbers.iter().copied().zip(rows.iter().cloned()).collect();
	let old_columns = decode_rows_to_columns(shape, &old_rows)?;
	let old_columns = decode_returning_dictionaries(exec.services, txn, &target.table.columns, old_columns)?;
	PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
		txn,
		target.namespace.name(),
		&target.table.name,
		DataOp::Update,
		&old_columns,
		PolicyTargetType::Table,
	)
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn rotate_table_pk_index(
	txn: &mut Transaction<'_>,
	table: &Table,
	shape: &RowShape,
	pk_def: &PrimaryKey,
	encoder: &PrimaryKeyEncoder,
	pre_row: &[u8],
	new_row: &[u8],
	row_number: RowNumber,
) -> Result<()> {
	let pre_key = encoder.encode(shape, pre_row);
	txn.remove(&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), pre_key))?;

	let post_key = encoder.encode(shape, new_row);
	txn.set(
		&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), post_key),
		EncodedPodRow::new(&u64::from(row_number).to_be_bytes()).into_bytes(),
	)?;
	Ok(())
}

#[inline]
fn update_table_result(namespace: &str, table: &str, updated: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
