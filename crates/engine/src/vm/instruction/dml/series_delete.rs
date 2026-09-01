// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{bytes::EncodedBytes, series::EncodedSeriesRow};
use reifydb_core::{
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			series::{Series, SeriesMetadata},
			storage::StorageId,
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedSeries},
	},
	key::{
		series::{PartitionedSeriesRowKey, SeriesRowKey},
		typed::key::Key,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{nodes::DeleteSeriesNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{Value, identity::IdentityId, row_number::RowNumber, system_columns::SystemColumns},
};
use tracing::instrument;

use super::{
	context::{SeriesTarget, WriteExecCtx},
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image},
};
use crate::{
	Result,
	error::EngineError,
	policy::PolicyEvaluator,
	transaction::operation::series::{apply_series_metadata_after_delete, remove_series_row},
	vm::{
		instruction::dml::shape::get_or_create_series_shape,
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
		},
	},
};

#[instrument(name = "mutate::series::delete", level = "trace", skip_all)]
pub(crate) fn delete_series(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteSeriesNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let DeleteSeriesNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, series, mut metadata) = resolve_delete_series_target(services, txn, &target)?;
	let target_data = SeriesTarget {
		namespace: &namespace,
		series: &series,
	};
	let has_tag = series.tag.is_some();
	let has_returning = returning.is_some();

	let exec = WriteExecCtx {
		services,
		symbols,
	};
	let input_plan = input.expect("DELETE on a series requires a filter pipeline");
	let (deleted_count, returned_rows) =
		run_series_delete_with_input(&exec, txn, *input_plan, &target_data, &params, has_tag, has_returning)?;

	if deleted_count > 0 {
		apply_series_metadata_after_delete(&mut metadata, deleted_count);
		services.catalog.update_series_metadata_txn(txn, series.id, metadata)?;
	}

	if let Some(returning_exprs) = &returning {
		let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;
		let mut cols = decode_rows_to_columns(&shape, &returned_rows);
		decode_returning_dictionaries(services, txn, &series.columns, &mut cols)?;
		let cols = with_pre_image(cols.clone(), &cols);
		return evaluate_returning(services, symbols, returning_exprs, cols, txn.identity());
	}
	Ok(delete_series_result(namespace.name(), &series.name, deleted_count))
}

#[inline]
fn resolve_delete_series_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedSeries,
) -> Result<(Namespace, Series, SeriesMetadata)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let series_name = target.name();
	let Some(series) = services.catalog.find_series_by_name(txn, namespace.id(), series_name)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};
	let Some(metadata) = services.catalog.find_series_metadata(txn, series.id)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};
	Ok((namespace, series, metadata))
}

fn run_series_delete_with_input(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	target: &SeriesTarget<'_>,
	params: &Params,
	has_tag: bool,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedBytes)>)> {
	let context = build_series_delete_query_context(exec, target, params, txn.identity());
	let mut input_node = compile_series_delete_input(txn, input_plan, &context)?;
	drive_series_delete_input(exec, txn, &mut input_node, &context, target, has_tag, has_returning)
}

#[inline]
fn build_series_delete_query_context(
	exec: &WriteExecCtx<'_>,
	target: &SeriesTarget<'_>,
	params: &Params,
	identity: IdentityId,
) -> QueryContext {
	let series = target.series;
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series.clone());
	QueryContext {
		services: exec.services.clone(),
		source: Some(ResolvedObject::Series(resolved_series)),
		batch_size: exec.services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity,
		memory: query_budget(exec.services),
	}
}

#[inline]
fn compile_series_delete_input(
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	context: &QueryContext,
) -> Result<Box<dyn QueryNode>> {
	let mut input_node = compile(input_plan, txn, Arc::new(context.clone()));
	input_node.initialize(txn, context)?;
	Ok(input_node)
}

#[inline]
fn drive_series_delete_input(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_node: &mut Box<dyn QueryNode>,
	context: &QueryContext,
	target: &SeriesTarget<'_>,
	has_tag: bool,
	has_returning: bool,
) -> Result<(u64, Vec<(RowNumber, EncodedBytes)>)> {
	let series = target.series;
	let mut deleted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let mut mutable_context = context.clone();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.row_count();
		if row_count == 0 {
			continue;
		}
		PolicyEvaluator::new(exec.services, exec.symbols).enforce_write_policies(
			txn,
			target.namespace.name(),
			&series.name,
			DataOp::Delete,
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_numbers = columns.row_numbers();
		reifydb_assertions! {
			let row_numbers_len = row_numbers.len();
			assert!(
				row_numbers_len == row_count,
				"series delete loop indexes row_numbers[0..row_count] but row_numbers.len()={row_numbers_len} != row_count={row_count}; \
				 a row batch without parallel row_numbers would panic out of bounds while building the delete key sequence"
			);
		}
		let partitioned = !series.partition_by.is_empty();
		if partitioned && columns.partitions().len() != row_count {
			return Err(EngineError::MissingPartitionAddress {
				object: ObjectId::series(series.id),
				operation: "DELETE",
			}
			.into());
		}
		for (row_idx, &row_number) in row_numbers.iter().enumerate() {
			let sequence = u64::from(row_number);
			let key_value = extract_series_delete_key_value(&columns, series, row_idx);
			let variant_tag = extract_series_delete_variant_tag(&columns, has_tag, row_idx);
			let encoded_key = if partitioned {
				PartitionedSeriesRowKey::encoded(
					StorageId::series(series.id),
					columns.partitions()[row_idx],
					variant_tag,
					key_value,
					sequence,
				)
			} else {
				SeriesRowKey {
					storage: StorageId::series(series.id),
					variant_tag,
					key: key_value,
					sequence,
				}
				.encode()
			};

			let Some(pre_entry) = txn.get(&encoded_key)? else {
				continue;
			};
			let encoded_bytes = pre_entry.bytes;
			let row_number = RowNumber::from(sequence);

			let committed = txn.get_committed(&encoded_key)?.map(|v| v.bytes);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| encoded_bytes.clone());

			let pre = build_series_delete_pre_columns_from_input(
				series,
				&columns,
				&pre_for_cdc,
				key_value,
				row_number,
				row_idx,
			);
			remove_series_row(txn, series, &encoded_key, pre_for_cdc, committed.is_some(), Some(pre))?;
			if has_returning {
				returned_rows.push((row_number, encoded_bytes));
			}
			deleted_count += 1;
		}
	}

	Ok((deleted_count, returned_rows))
}

#[inline]
fn extract_series_delete_key_value(columns: &Columns, series: &Series, row_idx: usize) -> u64 {
	columns.iter()
		.find(|c| c.name().text() == series.key.column())
		.and_then(|c| series.key_to_u64(c.data().get_value(row_idx)))
		.unwrap_or(0)
}

#[inline]
fn extract_series_delete_variant_tag(columns: &Columns, has_tag: bool, row_idx: usize) -> Option<u8> {
	if !has_tag {
		return None;
	}
	columns.iter().find(|c| c.name().text() == "tag").and_then(|c| match c.data().get_value(row_idx) {
		Value::Uint1(v) => Some(v),
		_ => None,
	})
}

fn build_series_delete_pre_columns_from_input(
	series: &Series,
	columns: &Columns,
	encoded_bytes: &EncodedBytes,
	key_value: u64,
	row_number: RowNumber,
	row_idx: usize,
) -> Columns {
	let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
	pre_col_vec.push(ColumnWithName::new(
		Fragment::internal(series.key.column()),
		series.key_column_data(vec![key_value]),
	));
	for col in columns.iter() {
		if col.name().text() != series.key.column() && col.name().text() != "tag" {
			let mut data = ColumnBuffer::with_capacity(col.data().get_type(), 1);
			data.push_value(col.data().get_value(row_idx));
			pre_col_vec.push(ColumnWithName {
				name: col.name().clone(),
				data,
			});
		}
	}
	Columns::with_system(
		pre_col_vec,
		SystemColumns::new(
			vec![row_number],
			Vec::new(),
			vec![EncodedSeriesRow::view(encoded_bytes).created_at()],
			vec![EncodedSeriesRow::view(encoded_bytes).updated_at()],
			EncodedSeriesRow::view(encoded_bytes).time().into_iter().collect(),
		),
	)
}

#[inline]
fn delete_series_result(namespace: &str, series: &str, deleted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}
