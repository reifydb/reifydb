// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	pod::EncodedPodRow,
	shape::RowShape,
	table::EncodedTableRowBuilder,
};
use reifydb_core::{
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		index::primary_key_violation,
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
	partition::{partition_col_indices, partition_of, partition_values},
	value::batch::single_row,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::InsertTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber, system_columns::user_columns},
};
use tracing::instrument;

use super::{
	columns::{CastColumns, ColumnPipeline, input_views, intern_dictionary_columns},
	context::TableTarget,
	primary_key::{self, PrimaryKeyEncoder},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image},
	shape::get_or_create_table_shape,
};
use crate::{
	Result,
	partition::resolve_partition,
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		instruction::dml::{coerce::InputFragments, time::resolve_time},
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

	let validated = validate_and_encode_input_rows(
		services,
		txn,
		&target_data,
		&shape,
		&context,
		symbols,
		&mut input_node,
		&fragments,
	)?;

	if !table.partition_by.is_empty() {
		let indices = partition_col_indices(&table.columns, &table.partition_by);
		let mut verified = HashSet::new();
		for row in &validated {
			let values = partition_values(&shape, row.as_slice(), &indices);
			let partition = partition_of(&table.columns, &table.partition_by, &values);
			resolve_partition(txn, ObjectId::Table(table.id), partition, &values, &mut verified)?;
		}
	}

	let total_rows = validated.len();
	if total_rows == 0 {
		return insert_table_result(namespace.name(), &table.name, 0);
	}

	let row_numbers = services.catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;
	assert_eq!(row_numbers.len(), validated.len());

	let pk_def = primary_key::get_primary_key(&services.catalog, txn, &table)?;
	let returned_rows = insert_validated_table_rows(
		txn,
		&target_data,
		&shape,
		validated,
		&row_numbers,
		returning.is_some(),
		pk_def.as_ref(),
	)?;

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &table.columns, columns)?;
		let columns = with_absent_pre_image(columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	insert_table_result(namespace.name(), &table.name, total_rows as u64)
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
) -> Result<Vec<EncodedTableRowBuilder>> {
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
		let mut cast = pipeline.cast_target_columns(&inputs, columns.num_rows(), None)?;
		pipeline.fill_sequences(services, txn, &mut cast)?;
		batches.push(cast);
	}
	intern_dictionary_columns(services, txn, &pipeline, &mut batches)?;

	let mut validated: Vec<EncodedTableRowBuilder> = Vec::new();
	for cast in &batches {
		let mut rows: Vec<EncodedTableRowBuilder> = (0..cast.rows()).map(|_| shape.allocate_table()).collect();
		cast.write(shape, &mut rows)?;
		for mut row in rows {
			let now = services.runtime_context.clock.now();
			row.set_timestamps(now, now);
			if let Some(time) = resolve_time(
				&target.table.name,
				&target.table.columns,
				&target.table.time,
				shape,
				&row,
				now,
			)? {
				row.set_time(time);
			}
			validated.push(row);
		}
	}
	Ok(validated)
}

fn insert_validated_table_rows(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	mut owned_rows: Vec<EncodedTableRowBuilder>,
	row_numbers: &[RowNumber],
	has_returning: bool,
	pk: Option<&PrimaryKey>,
) -> Result<Vec<(RowNumber, EncodedBytes)>> {
	txn.insert_table(target.table, shape, row_numbers, &mut owned_rows)?;

	if let Some(pk) = pk {
		let encoder = PrimaryKeyEncoder::new(pk, target.table)?;
		for (row, &row_number) in owned_rows.iter().zip(row_numbers.iter()) {
			write_insert_table_pk_index(txn, target, shape, pk, &encoder, row, row_number)?;
		}
	}

	if has_returning {
		Ok(row_numbers.iter().copied().zip(owned_rows.into_iter().map(|r| r.freeze_bytes())).collect())
	} else {
		Ok(Vec::new())
	}
}

#[inline]
fn write_insert_table_pk_index(
	txn: &mut Transaction<'_>,
	target: &TableTarget<'_>,
	shape: &RowShape,
	pk: &PrimaryKey,
	encoder: &PrimaryKeyEncoder,
	row: &[u8],
	row_number: RowNumber,
) -> Result<()> {
	let index_key = encoder.encode(shape, row);
	let index_entry_key = IndexEntryKey::new(target.table.id, IndexId::primary(pk.id), index_key.clone());
	if txn.contains(&index_entry_key)? {
		let key_columns = pk.columns.iter().map(|c| c.name.clone()).collect();
		return_error!(primary_key_violation(target.fragment.clone(), target.table.name.clone(), key_columns,));
	}
	txn.set(&index_entry_key, EncodedPodRow::new(&u64::from(row_number).to_be_bytes()).into_bytes())?;
	Ok(())
}

#[inline]
fn insert_table_result(namespace: &str, table: &str, inserted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("table", Value::Utf8(table.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}
