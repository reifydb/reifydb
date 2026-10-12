// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	shape::RowShape,
	table::EncodedTableRowBuilder,
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		query::column_not_found,
	},
	interface::{
		catalog::{
			column::Column,
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			table::Table,
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedTable},
	},
	partition::{BatchPartitions, partition_col_indices, partition_of, partitions_of_batch},
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::InsertTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{
		Value, identity::IdentityId, partition::Partition, row_number::RowNumber, system_columns::user_columns,
	},
};
use tracing::instrument;

use super::{
	columns::{CastColumns, ColumnPipeline, input_views, intern_dictionary_columns},
	context::TableTarget,
	primary_key::{self, PrimaryKeyEncoder, Violation, checked_primary_keys, primary_key_index},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	partition::resolve_partitions,
	policy::PolicyEvaluator,
	transaction::operation::table::{TableInsert, TableOperations},
	vm::{
		instruction::dml::{
			coerce::InputFragments,
			time::{RowTime, populator_index},
		},
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

#[instrument(name = "mutate::table::insert", level = "trace", skip_all)]
pub(crate) fn insert_table(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertTableNode,
	symbols: &mut SymbolTable,
) -> Result<RecordBatch> {
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
	let context = build_insert_table_query_context(services, &target_data, symbols, txn.identity());
	let fragments = InputFragments::of(&input);
	let mut input_node = compile(*input, txn, context.clone());
	input_node.initialize(txn, &context)?;

	let mut encoded = validate_and_encode_input_rows(
		services,
		txn,
		&target_data,
		&shape,
		&context,
		symbols,
		&mut input_node,
		&fragments,
	)?;
	let row_numbers = write_table_rows(
		&services.catalog,
		txn,
		&table,
		&shape,
		&mut encoded,
		&Violation::At(target_data.fragment),
	)?;
	let total_rows = encoded.rows.len();
	if total_rows == 0 {
		return insert_table_result(namespace.name(), &table.name, 0);
	}

	if let Some(returning_exprs) = &returning {
		let returned_rows: Vec<(RowNumber, EncodedBytes)> = row_numbers
			.iter()
			.copied()
			.zip(encoded.rows.into_iter().map(|row| row.freeze_bytes()))
			.collect();
		let columns = decode_rows_to_columns(&shape, &returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &table.columns, columns)?;
		let columns = with_absent_pre_image(columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	insert_table_result(namespace.name(), &table.name, total_rows as u64)
}

#[derive(Default)]
pub(crate) struct EncodedTableRows {
	pub(crate) rows: Vec<EncodedTableRowBuilder>,
	pub(crate) partitions: Vec<Partition>,
	pub(crate) distinct: Vec<(Partition, Vec<Value>)>,
}

pub(crate) fn table_batch_partitions(table: &Table, cast: &CastColumns, encoded: &mut EncodedTableRows) -> Result<()> {
	let batch = cast_partitions(&table.columns, &table.partition_by, cast)?;
	encoded.partitions.extend(batch.rows);
	encoded.distinct.extend(batch.distinct);
	Ok(())
}

pub(crate) fn cast_partitions(
	columns: &[Column],
	partition_by: &[String],
	cast: &CastColumns,
) -> Result<BatchPartitions> {
	let indices = partition_col_indices(columns, partition_by);
	if indices.is_empty() {
		return Ok(BatchPartitions::default());
	}
	let keys = indices.iter().map(|&index| cast.view(index)).collect::<Result<Vec<_>>>()?;
	let batch = partitions_of_batch(&keys);
	reifydb_assertions! {
		for (partition, values) in &batch.distinct {
			assert_eq!(
				partition_of(columns, partition_by, values),
				*partition,
				"a batch partition must hash its values like the row-at-a-time partition"
			);
		}
	}
	Ok(batch)
}

pub(crate) fn write_table_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	table: &Table,
	shape: &RowShape,
	encoded: &mut EncodedTableRows,
	violation: &Violation<'_>,
) -> Result<Vec<RowNumber>> {
	resolve_partitions(txn, ObjectId::Table(table.id), &encoded.distinct, &mut HashMap::new())?;
	if encoded.rows.is_empty() {
		return Ok(Vec::new());
	}

	let pk_def = primary_key::get_primary_key(catalog, txn, table)?;
	let checked = match &pk_def {
		Some(pk) => {
			let encoder = PrimaryKeyEncoder::new(pk, table)?;
			let keys = checked_primary_keys(txn, table, pk, &encoder, shape, &encoded.rows, violation)?;
			Some((pk, encoder.key_columns(), keys))
		}
		None => None,
	};

	let row_numbers = catalog.next_row_number_batch(txn, table.id, encoded.rows.len() as u64)?;
	assert_eq!(row_numbers.len(), encoded.rows.len());

	let (index, key_columns) = match checked {
		Some((pk, key_columns, keys)) => (primary_key_index(table, pk, keys, &row_numbers), key_columns),
		None => (Vec::new(), Vec::new()),
	};
	txn.insert_table(
		table,
		shape,
		TableInsert {
			ids: &row_numbers,
			partitions: &encoded.partitions,
			rows: &mut encoded.rows,
			index,
			key_columns: &key_columns,
		},
	)?;
	Ok(row_numbers)
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
	identity: IdentityId,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let table_ident = Fragment::internal(target.table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, target.table.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Table(resolved_table)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: Params::None,
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	})
}

#[allow(clippy::too_many_arguments)]
fn validate_and_encode_input_rows(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	context: &Arc<QueryContext>,
	symbols: &SymbolTable,
	input_node: &mut Box<dyn QueryNode>,
	fragments: &InputFragments,
) -> Result<EncodedTableRows> {
	let pipeline = ColumnPipeline {
		columns: &target.table.columns,
		sequences: Some(target.table.id.into()),
		series_key: None,
		fragments,
		context,
	};
	let mut batches: Vec<CastColumns> = Vec::new();
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
		if let Some((unknown, _)) = user_columns(&columns)
			.find(|(field, _)| !target.table.columns.iter().any(|c| &c.name == field.name()))
		{
			return_error!(column_not_found(fragments.column(unknown.name())));
		}
		let inputs = input_views(&columns, &target.table.columns)?;
		batches.push(pipeline.cast_target_columns(&inputs, columns.num_rows(), None)?);
	}
	intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut batches)?;
	pipeline.fill_sequences(services, txn, &mut batches)?;

	let populator = populator_index(&target.table.time, shape);
	let mut encoded = EncodedTableRows::default();
	for cast in &batches {
		let mut rows: Vec<EncodedTableRowBuilder> = (0..cast.rows()).map(|_| shape.allocate_table()).collect();
		cast.write(shape, &mut rows)?;
		let now = services.runtime_context.clock.now();
		let times = RowTime::new(
			&target.table.name,
			&target.table.time,
			shape,
			populator.map(|index| cast.view(index)).transpose()?,
		);
		for (index, mut row) in rows.into_iter().enumerate() {
			row.set_timestamps(now, now);
			if let Some(time) = times.inserted(index, &row, now)? {
				row.set_time(time);
			}
			encoded.rows.push(row);
		}
		table_batch_partitions(target.table, cast, &mut encoded)?;
	}
	Ok(encoded)
}

#[inline]
fn insert_table_result(namespace: &str, table: &str, inserted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}
