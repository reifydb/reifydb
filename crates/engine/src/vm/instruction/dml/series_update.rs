// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			series::Series,
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::nodes::UpdateSeriesNode;
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::{context::SeriesTarget, returning::evaluate_returning};
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

#[instrument(name = "mutate::series::update", level = "trace", skip_all)]
pub(crate) fn update_series(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateSeriesNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let UpdateSeriesNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, series) = resolve_update_series_target(services, txn, &target)?;
	let target_data = SeriesTarget {
		namespace: &namespace,
		series: &series,
	};
	let context = build_update_series_query_context(services, &target_data, &params, symbols);
	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let has_tag = series.tag.is_some();
	let mut updated_count = 0u64;
	let mut returning_columns: Option<Columns> = None;

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.row_count();
		if row_count == 0 {
			continue;
		}

		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&series.name,
			DataOp::Update,
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_numbers = columns.row_numbers.clone();
		let updates_to_apply =
			build_series_updates_to_apply(services, txn, &series, &columns, &row_numbers, has_tag)?;

		for (encoded_key, mut row, row_idx) in updates_to_apply {
			let pre_values = txn.get(&encoded_key)?.map(|v| v.row);
			let old_created_at = pre_values.as_ref().expect("row must exist for update").created_at_nanos();
			row.set_timestamps(old_created_at, services.runtime_context.clock.now_nanos());

			let key_value = extract_series_update_key_value(&columns, &series, row_idx);
			let row_number = RowNumber::from(u64::from(row_numbers[row_idx]));

			if let Some(ref pre_vals) = pre_values {
				let event = SeriesUpdateEvent {
					columns: &columns,
					pre: pre_vals,
					post: &row,
					key_value,
					row_number,
					row_idx,
				};
				track_series_update_flow_change(services, txn, &series, &event)?;
			}

			let pre_for_interceptor = pre_values.clone().unwrap_or_else(|| row.clone());
			let row = SeriesRowInterceptor::pre_update(txn, &series, row.clone())?;
			txn.set(&encoded_key, row.clone())?;
			SeriesRowInterceptor::post_update(txn, &series, &row, &pre_for_interceptor)?;
			updated_count += 1;
		}

		if returning.is_some() {
			returning_columns = Some(accumulate_returning_columns(returning_columns, columns));
		}
	}

	if let Some(returning_exprs) = &returning {
		let cols = returning_columns.unwrap_or_else(Columns::empty);
		return evaluate_returning(services, symbols, returning_exprs, cols);
	}
	Ok(update_series_result(namespace.name(), &series.name, updated_count))
}

/// Pre + post snapshot of a series row update, used for flow-change tracking.
struct SeriesUpdateEvent<'a> {
	columns: &'a Columns,
	pre: &'a EncodedRow,
	post: &'a EncodedRow,
	key_value: u64,
	row_number: RowNumber,
	row_idx: usize,
}

#[inline]
fn resolve_update_series_target(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &ResolvedSeries,
) -> Result<(Namespace, Series)> {
	let namespace_name = target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};
	let series_name = target.name();
	let Some(series) = services.catalog.find_series_by_name(txn, namespace.id(), series_name)? else {
		let fragment = Fragment::internal(target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};
	Ok((namespace, series))
}

#[inline]
fn build_update_series_query_context(
	services: &Arc<Services>,
	target: &SeriesTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(target.series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, target.series.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::Series(resolved_series)),
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	}
}

fn build_series_updates_to_apply(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	series: &Series,
	columns: &Columns,
	row_numbers: &[RowNumber],
	has_tag: bool,
) -> Result<Vec<(EncodedKey, EncodedRow, usize)>> {
	let row_count = columns.row_count();
	let mut updates_to_apply: Vec<(EncodedKey, EncodedRow, usize)> = Vec::with_capacity(row_count);
	for (row_idx, row_number) in row_numbers.iter().enumerate().take(row_count) {
		let sequence = u64::from(*row_number);
		let key_value = extract_series_update_key_value(columns, series, row_idx);
		let variant_tag = extract_series_update_variant_tag(columns, has_tag, row_idx);

		let key = SeriesRowKey {
			series: series.id,
			variant_tag,
			key: key_value,
			sequence,
		};
		let encoded_key = key.encode();

		let shape = get_or_create_series_shape(&services.catalog, series, txn)?;
		let row = build_series_update_row(series, columns, &shape, row_idx);
		updates_to_apply.push((encoded_key, row, row_idx));
	}
	Ok(updates_to_apply)
}

#[inline]
fn extract_series_update_key_value(columns: &Columns, series: &Series, row_idx: usize) -> u64 {
	columns.iter()
		.find(|c| c.name().text() == series.key.column())
		.and_then(|c| series.key_to_u64(c.data().get_value(row_idx)))
		.unwrap_or(0)
}

#[inline]
fn extract_series_update_variant_tag(columns: &Columns, has_tag: bool, row_idx: usize) -> Option<u8> {
	if !has_tag {
		return None;
	}
	columns.iter().find(|c| c.name().text() == "tag").and_then(|c| match c.data().get_value(row_idx) {
		Value::Uint1(v) => Some(v),
		_ => None,
	})
}

#[inline]
fn build_series_update_row(series: &Series, columns: &Columns, shape: &RowShape, row_idx: usize) -> EncodedRow {
	let mut row = shape.allocate();
	let key_col_value = columns
		.iter()
		.find(|c| c.name().text() == series.key.column())
		.map(|c| c.data().get_value(row_idx))
		.unwrap_or(Value::Int8(0));
	shape.set_value(&mut row, 0, &key_col_value);

	for (i, col_def) in series.data_columns().enumerate() {
		let value = columns
			.iter()
			.find(|c| c.name().text() == col_def.name)
			.map(|c| c.data().get_value(row_idx))
			.unwrap_or(Value::none());
		shape.set_value(&mut row, i + 1, &value);
	}
	row
}

fn track_series_update_flow_change(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	series: &Series,
	event: &SeriesUpdateEvent<'_>,
) -> Result<()> {
	let read_shape = get_or_create_series_shape(&services.catalog, series, txn)?;
	let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
	pre_col_vec.push(ColumnWithName::new(
		Fragment::internal(series.key.column()),
		series.key_column_data(vec![event.key_value]),
	));
	for (i, col_def) in series.data_columns().enumerate() {
		let val = read_shape.get_value(event.pre, i + 1);
		let mut data = ColumnBuffer::with_capacity(col_def.constraint.get_type(), 1);
		data.push_value(val);
		pre_col_vec.push(ColumnWithName {
			name: Fragment::internal(&col_def.name),
			data,
		});
	}

	let mut post_col_vec = Vec::with_capacity(1 + series.columns.len());
	post_col_vec.push(ColumnWithName::new(
		Fragment::internal(series.key.column()),
		series.key_column_data(vec![event.key_value]),
	));
	for col in event.columns.iter() {
		if col.name().text() != series.key.column() && col.name().text() != "tag" {
			let mut data = ColumnBuffer::with_capacity(col.data().get_type(), 1);
			data.push_value(col.data().get_value(event.row_idx));
			post_col_vec.push(ColumnWithName {
				name: col.name().clone(),
				data,
			});
		}
	}

	let pre = Columns::with_system_columns(
		pre_col_vec,
		vec![event.row_number],
		vec![DateTime::from_nanos(event.pre.created_at_nanos())],
		vec![DateTime::from_nanos(event.pre.updated_at_nanos())],
	);
	let post = Columns::with_system_columns(
		post_col_vec,
		vec![event.row_number],
		vec![DateTime::from_nanos(event.post.created_at_nanos())],
		vec![DateTime::from_nanos(event.post.updated_at_nanos())],
	);
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::update(pre, post)],
		changed_at: DateTime::default(),
	});
	Ok(())
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
			Columns::new(cols)
		}
		None => columns,
	}
}

#[inline]
fn update_series_result(namespace: &str, series: &str, updated: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
