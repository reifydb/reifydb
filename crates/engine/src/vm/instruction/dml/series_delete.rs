// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			series::{Series, SeriesMetadata},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{nodes::DeleteSeriesNode, query::QueryPlan};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	context::{SeriesTarget, WriteExecCtx},
	returning::{decode_rows_to_columns, evaluate_returning},
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	vm::{
		instruction::dml::shape::get_or_create_series_shape,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
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
	let (deleted_count, returning_columns) = if let Some(input_plan) = input {
		run_series_delete_with_input(&exec, txn, *input_plan, &target_data, &params, has_tag, has_returning)?
	} else {
		run_series_delete_all(services, txn, &target_data, has_returning)?
	};

	if deleted_count > 0 {
		update_series_metadata_after_delete(&mut metadata, deleted_count);
		services.catalog.update_series_metadata_txn(txn, metadata)?;
	}

	if let Some(returning_exprs) = &returning {
		let cols = returning_columns.unwrap_or_else(Columns::empty);
		return evaluate_returning(services, symbols, returning_exprs, cols);
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
) -> Result<(u64, Option<Columns>)> {
	let series = target.series;
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series.clone());
	let context = QueryContext {
		services: exec.services.clone(),
		source: Some(ResolvedShape::Series(resolved_series)),
		batch_size: exec.services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: exec.symbols.clone(),
		identity: IdentityId::root(),
	};

	let mut input_node = compile(input_plan, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let mut deleted_count = 0u64;
	let mut returning_columns: Option<Columns> = None;
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

		let row_numbers = columns.row_numbers.clone();
		for row_idx in 0..row_count {
			let sequence = u64::from(row_numbers[row_idx]);
			let key_value = extract_series_delete_key_value(&columns, series, row_idx);
			let variant_tag = extract_series_delete_variant_tag(&columns, has_tag, row_idx);
			let key = SeriesRowKey {
				series: series.id,
				variant_tag,
				key: key_value,
				sequence,
			};
			let encoded_key = key.encode();

			let Some(pre_entry) = txn.get(&encoded_key)? else {
				continue;
			};
			let encoded_row = pre_entry.row;
			let row_number = RowNumber::from(sequence);

			let committed = txn.get_committed(&encoded_key)?.map(|v| v.row);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| encoded_row.clone());

			let pre = build_series_delete_pre_columns_from_input(
				series,
				&columns,
				&pre_for_cdc,
				key_value,
				row_number,
				row_idx,
			);
			emit_series_remove_change(txn, series, pre);

			SeriesRowInterceptor::pre_delete(txn, series)?;
			if committed.is_some() {
				txn.mark_preexisting(&encoded_key)?;
			}
			txn.unset(&encoded_key, pre_for_cdc.clone())?;
			SeriesRowInterceptor::post_delete(txn, series, &pre_for_cdc)?;
			deleted_count += 1;
		}

		if has_returning {
			returning_columns = Some(accumulate_returning_columns(returning_columns, columns));
		}
	}

	if has_returning && returning_columns.is_none() {
		returning_columns = Some(Columns::empty());
	}
	Ok((deleted_count, returning_columns))
}

fn run_series_delete_all(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &SeriesTarget<'_>,
	has_returning: bool,
) -> Result<(u64, Option<Columns>)> {
	let series = target.series;
	let range = SeriesRowKeyRange::full_scan(series.id, None);
	let mut entries_to_delete: Vec<(EncodedKey, EncodedRow)> = Vec::new();

	let mut stream = txn.range(range, 32)?;
	for entry in stream.by_ref() {
		let entry = entry?;
		entries_to_delete.push((entry.key, entry.row));
	}
	drop(stream);

	let delete_all_shape = get_or_create_series_shape(&services.catalog, series, txn)?;
	let mut deleted_count = 0u64;

	for (key, encoded_row) in entries_to_delete.iter() {
		let committed = txn.get_committed(key)?.map(|v| v.row);
		let pre_for_cdc = committed.clone().unwrap_or_else(|| encoded_row.clone());

		if let Some(decoded_key) = SeriesRowKey::decode(key) {
			let pre = build_series_delete_pre_columns_from_storage(
				series,
				&delete_all_shape,
				&pre_for_cdc,
				&decoded_key,
			);
			emit_series_remove_change(txn, series, pre);
		}

		SeriesRowInterceptor::pre_delete(txn, series)?;
		if committed.is_some() {
			txn.mark_preexisting(key)?;
		}
		txn.unset(key, pre_for_cdc.clone())?;
		SeriesRowInterceptor::post_delete(txn, series, &pre_for_cdc)?;
		deleted_count += 1;
	}

	let returning_columns = if has_returning {
		let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
		for (key, encoded) in entries_to_delete.iter() {
			if let Some(decoded_key) = SeriesRowKey::decode(key) {
				returned_rows.push((RowNumber::from(decoded_key.sequence), encoded.clone()));
			}
		}
		Some(decode_rows_to_columns(&delete_all_shape, &returned_rows))
	} else {
		None
	};
	Ok((deleted_count, returning_columns))
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
	encoded_row: &EncodedRow,
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
	Columns::with_system_columns(
		pre_col_vec,
		vec![row_number],
		vec![DateTime::from_nanos(encoded_row.created_at_nanos())],
		vec![DateTime::from_nanos(encoded_row.updated_at_nanos())],
	)
}

fn build_series_delete_pre_columns_from_storage(
	series: &Series,
	shape: &RowShape,
	encoded_row: &EncodedRow,
	decoded_key: &SeriesRowKey,
) -> Columns {
	let row_number = RowNumber::from(decoded_key.sequence);
	let data_values: Vec<Value> =
		series.data_columns().enumerate().map(|(i, _)| shape.get_value(encoded_row, i + 1)).collect();
	let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
	pre_col_vec.push(ColumnWithName::new(
		Fragment::internal(series.key.column()),
		series.key_column_data(vec![decoded_key.key]),
	));
	for (col_idx, col_def) in series.data_columns().enumerate() {
		let mut data = ColumnBuffer::with_capacity(col_def.constraint.get_type(), 1);
		data.push_value(data_values.get(col_idx).cloned().unwrap_or(Value::none()));
		pre_col_vec.push(ColumnWithName {
			name: Fragment::internal(&col_def.name),
			data,
		});
	}
	Columns::with_system_columns(
		pre_col_vec,
		vec![row_number],
		vec![DateTime::from_nanos(encoded_row.created_at_nanos())],
		vec![DateTime::from_nanos(encoded_row.updated_at_nanos())],
	)
}

#[inline]
fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: Columns) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

fn accumulate_returning_columns(returning_columns: Option<Columns>, columns: Columns) -> Columns {
	match returning_columns {
		Some(existing) => {
			let mut cols = Vec::new();
			for (i, col) in columns.iter().enumerate() {
				if let Some(existing_col) = existing.get(i) {
					let mut data = ColumnBuffer::with_capacity(
						col.data().get_type(),
						existing_col.data().len() + col.data().len(),
					);
					for j in 0..existing_col.data().len() {
						data.push_value(existing_col.data().get_value(j));
					}
					for j in 0..col.data().len() {
						data.push_value(col.data().get_value(j));
					}
					cols.push(ColumnWithName {
						name: col.name().clone(),
						data,
					});
				}
			}
			let mut row_numbers = existing.row_numbers.to_vec();
			row_numbers.extend(columns.row_numbers.iter().copied());
			let mut created_at = existing.created_at.to_vec();
			created_at.extend(columns.created_at.iter().copied());
			let mut updated_at = existing.updated_at.to_vec();
			updated_at.extend(columns.updated_at.iter().copied());
			Columns::with_system_columns(cols, row_numbers, created_at, updated_at)
		}
		None => columns,
	}
}

#[inline]
fn update_series_metadata_after_delete(metadata: &mut SeriesMetadata, deleted_count: u64) {
	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}
}

#[inline]
fn delete_series_result(namespace: &str, series: &str, deleted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}
