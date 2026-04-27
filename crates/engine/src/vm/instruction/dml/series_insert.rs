// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, shape::RowShape},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			column::Column,
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			series::{Series, SeriesKey, SeriesMetadata, TimestampPrecision},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::nodes::InsertSeriesNode;
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::{
	context::SeriesTarget,
	returning::{decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_series_shape,
};
use crate::{
	Result,
	policy::PolicyEvaluator,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

#[instrument(name = "mutate::series::insert", level = "trace", skip_all)]
pub(crate) fn insert_series(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertSeriesNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let InsertSeriesNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, series, mut metadata) = resolve_insert_series_target(services, txn, &target)?;
	let target_data = SeriesTarget {
		namespace: &namespace,
		series: &series,
	};
	let context = build_insert_series_query_context(services, &target_data, &params, symbols);
	let mut input_node = compile(*input, txn, context.clone());

	let has_tag = series.tag.is_some();
	let key_column_name = series.key.column();

	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = if returning.is_some() {
		Vec::with_capacity(16)
	} else {
		Vec::new()
	};
	let has_returning = returning.is_some();

	input_node.initialize(txn, &context)?;
	let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;

	let mut mutable_context = (*context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace.name(),
			&series.name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_count = columns.row_count();
		for row_idx in 0..row_count {
			let key_value = extract_or_generate_series_key(
				services,
				&columns,
				&series,
				&metadata,
				row_idx,
				key_column_name,
			);
			let variant_tag = extract_variant_tag(&columns, has_tag, row_idx);

			metadata.sequence_counter += 1;
			let sequence = metadata.sequence_counter;
			let row_key = SeriesRowKey {
				series: series.id,
				variant_tag,
				key: key_value,
				sequence,
			};
			let encoded_key = row_key.encode();

			let data_columns: Vec<_> = series.data_columns().collect();
			let data_values = collect_series_data_values(&columns, &data_columns, row_idx);
			let row = build_encoded_series_row(services, &series, &shape, key_value, &data_values);

			let row = SeriesRowInterceptor::pre_insert(txn, &series, row)?;
			txn.set(&encoded_key, row.clone())?;
			SeriesRowInterceptor::post_insert(txn, &series, &row)?;

			if has_returning {
				returned_rows.push((RowNumber::from(sequence), row.clone()));
			}

			let snapshot = SeriesRowSnapshot {
				key_column_name,
				key_value,
				data_columns: &data_columns,
				data_values: &data_values,
				sequence,
				row: &row,
			};
			track_series_insert_flow_change(txn, &series, &snapshot);

			update_series_metadata_for_insert(&mut metadata, key_value);
			inserted_count += 1;
		}
	}

	if inserted_count > 0 {
		services.catalog.update_series_metadata_txn(txn, metadata)?;
	}

	if let Some(returning_exprs) = &returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(insert_series_result(namespace.name(), &series.name, inserted_count))
}

/// Snapshot of one freshly-inserted series row, used for flow-change tracking.
struct SeriesRowSnapshot<'a> {
	key_column_name: &'a str,
	key_value: u64,
	data_columns: &'a [&'a Column],
	data_values: &'a [Value],
	sequence: u64,
	row: &'a EncodedRow,
}

#[inline]
fn resolve_insert_series_target(
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

#[inline]
fn build_insert_series_query_context(
	services: &Arc<Services>,
	target: &SeriesTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(target.series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, target.series.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedShape::Series(resolved_series)),
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	})
}

#[inline]
fn extract_or_generate_series_key(
	services: &Arc<Services>,
	columns: &Columns,
	series: &Series,
	metadata: &SeriesMetadata,
	row_idx: usize,
	key_column_name: &str,
) -> u64 {
	let from_input = columns
		.iter()
		.find(|col| col.name().text() == key_column_name)
		.and_then(|key_col| series.key_to_u64(key_col.data().get_value(row_idx)));
	match from_input {
		Some(v) => v,
		None => match &series.key {
			SeriesKey::DateTime {
				precision,
				..
			} => generate_timestamp(services, precision),
			SeriesKey::Integer {
				..
			} => metadata.newest_key + 1,
		},
	}
}

#[inline]
fn extract_variant_tag(columns: &Columns, has_tag: bool, row_idx: usize) -> Option<u8> {
	if !has_tag {
		return None;
	}
	let Some(tag_col) = columns.iter().find(|col| col.name().text() == "tag") else {
		return Some(0);
	};
	match tag_col.data().get_value(row_idx) {
		Value::Uint1(t) => Some(t),
		Value::Int1(t) => Some(t as u8),
		_ => Some(0),
	}
}

#[inline]
fn collect_series_data_values(columns: &Columns, data_columns: &[&Column], row_idx: usize) -> Vec<Value> {
	let mut values = Vec::with_capacity(data_columns.len());
	for col_def in data_columns {
		let value = if let Some(input_col) = columns.iter().find(|c| c.name().text() == col_def.name) {
			input_col.data().get_value(row_idx)
		} else {
			Value::none()
		};
		values.push(value);
	}
	values
}

#[inline]
fn build_encoded_series_row(
	services: &Arc<Services>,
	series: &Series,
	shape: &RowShape,
	key_value: u64,
	data_values: &[Value],
) -> EncodedRow {
	let key_value_encoded = series.key_from_u64(key_value);
	let mut row = shape.allocate();
	shape.set_value(&mut row, 0, &key_value_encoded);
	for (i, value) in data_values.iter().enumerate() {
		shape.set_value(&mut row, i + 1, value);
	}
	let now_nanos = services.runtime_context.clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	row
}

fn track_series_insert_flow_change(txn: &mut Transaction<'_>, series: &Series, snapshot: &SeriesRowSnapshot<'_>) {
	let row_number = RowNumber::from(snapshot.sequence);
	let mut cols = Vec::with_capacity(1 + snapshot.data_columns.len());
	cols.push(ColumnWithName::new(
		Fragment::internal(snapshot.key_column_name),
		series.key_column_data(vec![snapshot.key_value]),
	));
	for (i, col_def) in snapshot.data_columns.iter().enumerate() {
		let mut data = ColumnBuffer::with_capacity(col_def.constraint.get_type(), 1);
		data.push_value(snapshot.data_values[i].clone());
		cols.push(ColumnWithName {
			name: Fragment::internal(&col_def.name),
			data,
		});
	}
	let post = Columns::with_system_columns(
		cols,
		vec![row_number],
		vec![DateTime::from_nanos(snapshot.row.created_at_nanos())],
		vec![DateTime::from_nanos(snapshot.row.updated_at_nanos())],
	);
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
		version: CommitVersion(0),
		diffs: vec![Diff::insert(post)],
		changed_at: DateTime::default(),
	});
}

#[inline]
fn update_series_metadata_for_insert(metadata: &mut SeriesMetadata, key_value: u64) {
	if metadata.row_count == 0 {
		metadata.oldest_key = key_value;
		metadata.newest_key = key_value;
	} else {
		if key_value < metadata.oldest_key {
			metadata.oldest_key = key_value;
		}
		if key_value > metadata.newest_key {
			metadata.newest_key = key_value;
		}
	}
	metadata.row_count += 1;
}

#[inline]
fn insert_series_result(namespace: &str, series: &str, inserted: u64) -> Columns {
	Columns::single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}

fn generate_timestamp(services: &Services, precision: &TimestampPrecision) -> u64 {
	match precision {
		TimestampPrecision::Second => services.runtime_context.clock.now_secs(),
		TimestampPrecision::Millisecond => services.runtime_context.clock.now_millis(),
		TimestampPrecision::Microsecond => services.runtime_context.clock.now_micros(),
		TimestampPrecision::Nanosecond => services.runtime_context.clock.now_nanos(),
	}
}
