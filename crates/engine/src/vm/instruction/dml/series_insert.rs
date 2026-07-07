// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			column::Column,
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			policy::{DataOp, PolicyTargetType},
			series::{Series, SeriesKey, SeriesMetadata, TimestampPrecision},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	internal_error,
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		series_row::SeriesRowKey,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{expression::Expression, nodes::InsertSeriesNode};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{Value, datetime::DateTime, identity::IdentityId, partition::Partition, row_number::RowNumber},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	context::SeriesTarget,
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning},
	shape::get_or_create_series_shape,
};
use crate::{
	Result,
	partition::resolve_partition,
	policy::PolicyEvaluator,
	transaction::operation::dictionary::DictionaryOperations,
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
	let context = build_insert_series_query_context(
		services,
		&SeriesTarget {
			namespace: &namespace,
			series: &series,
		},
		&params,
		symbols,
	);
	let mut input_node = compile(*input, txn, context.clone());

	let has_tag = series.tag.is_some();
	let key_column_name = series.key.column();
	let has_returning = returning.is_some();
	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = if has_returning {
		Vec::with_capacity(16)
	} else {
		Vec::new()
	};

	input_node.initialize(txn, &context)?;
	let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;

	let mut mutable_context = (*context).clone();
	let mut verified: HashSet<Partition> = HashSet::new();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		enforce_series_write_policies(services, symbols, txn, &namespace, &series, &columns)?;
		for row_idx in 0..columns.row_count() {
			insert_series_row(
				services,
				txn,
				&series,
				&mut metadata,
				&shape,
				&columns,
				row_idx,
				key_column_name,
				has_tag,
				has_returning,
				&mut returned_rows,
				&mut verified,
			)?;
			inserted_count += 1;
		}
	}

	reifydb_assertions! {
		let collected = returned_rows.len() as u64;
		assert!(
			!has_returning || collected == inserted_count,
			"each inserted series row must contribute exactly one RETURNING row; a mismatch means \
			 decode_rows_to_columns would emit a row count that disagrees with what was committed, \
			 silently corrupting the RETURNING result (inserted={inserted_count}, collected={collected})"
		);
	}

	finalize_series_insert(
		services,
		txn,
		symbols,
		&namespace,
		&series,
		&shape,
		metadata,
		inserted_count,
		&returning,
		&returned_rows,
	)
}

fn enforce_series_write_policies(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	txn: &mut Transaction<'_>,
	namespace: &Namespace,
	series: &Series,
	columns: &Columns,
) -> Result<()> {
	PolicyEvaluator::new(services, symbols).enforce_write_policies(
		txn,
		namespace.name(),
		&series.name,
		DataOp::Insert,
		columns,
		PolicyTargetType::Series,
	)
}

#[allow(clippy::too_many_arguments)]
fn insert_series_row(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	series: &Series,
	metadata: &mut SeriesMetadata,
	shape: &RowShape,
	columns: &Columns,
	row_idx: usize,
	key_column_name: &str,
	has_tag: bool,
	has_returning: bool,
	returned_rows: &mut Vec<(RowNumber, EncodedRow)>,
	verified: &mut HashSet<Partition>,
) -> Result<()> {
	let key_value = extract_or_generate_series_key(services, columns, series, metadata, row_idx, key_column_name);
	let variant_tag = extract_variant_tag(columns, has_tag, row_idx);

	metadata.sequence_counter += 1;
	let sequence = metadata.sequence_counter;
	let encoded_key = if series.partition_by.is_empty() {
		SeriesRowKey {
			series: series.id,
			variant_tag,
			key: key_value,
			sequence,
		}
		.encode()
	} else {
		let mut part_values = Vec::with_capacity(series.partition_by.len());
		for name in &series.partition_by {
			let idx = columns.names.iter().position(|n| n.text() == name.as_str()).ok_or_else(|| {
				internal_error!("partition column {} missing from series insert input", name)
			})?;
			part_values.push(columns[idx].get_value(row_idx));
		}
		let partition = Partition::of(&part_values);
		resolve_partition(txn, ShapeId::Series(series.id), partition, &part_values, verified)?;
		PartitionedRowKey::encoded(
			ShapeId::Series(series.id),
			partition,
			RowLocator::Series {
				variant_tag,
				key: key_value,
				sequence,
			},
		)
	};

	let data_columns: Vec<_> = series.data_columns().collect();
	let data_values = collect_series_data_values(columns, &data_columns, row_idx);
	let mut encoded_values = data_values.clone();
	for (i, col_def) in data_columns.iter().enumerate() {
		if let Some(dict_id) = col_def.dictionary_id {
			let dictionary = services.catalog.find_dictionary(txn, dict_id)?.ok_or_else(|| {
				internal_error!("Dictionary {:?} not found for column {}", dict_id, col_def.name)
			})?;
			let entry_id = if matches!(encoded_values[i], Value::None { .. }) {
				dictionary.id_type.none()
			} else {
				txn.insert_into_dictionary(&dictionary, &encoded_values[i])?
			};
			encoded_values[i] = entry_id.to_value();
		}
	}
	let row = build_encoded_series_row(services, series, shape, key_value, &encoded_values);

	let mut rows_buf = [row];
	SeriesRowInterceptor::pre_insert(txn, series, &mut rows_buf)?;
	let [row] = rows_buf;
	txn.set(&encoded_key, row.clone())?;
	let rows = [row.clone()];
	SeriesRowInterceptor::post_insert(txn, series, &rows)?;

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
	track_series_insert_flow_change(txn, series, &snapshot);

	update_series_metadata_for_insert(metadata, key_value);
	Ok(())
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn finalize_series_insert(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
	namespace: &Namespace,
	series: &Series,
	shape: &RowShape,
	metadata: SeriesMetadata,
	inserted_count: u64,
	returning: &Option<Vec<Expression>>,
	returned_rows: &[(RowNumber, EncodedRow)],
) -> Result<Columns> {
	if inserted_count > 0 {
		services.catalog.update_series_metadata_txn(txn, metadata)?;
	}

	if let Some(returning_exprs) = returning {
		let mut columns = decode_rows_to_columns(shape, returned_rows);
		decode_returning_dictionaries(services, txn, &series.columns, &mut columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}
	Ok(insert_series_result(namespace.name(), &series.name, inserted_count))
}

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
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
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
		diffs: smallvec![Diff::insert(post)],
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
