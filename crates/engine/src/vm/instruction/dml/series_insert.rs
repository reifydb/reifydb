// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	sync::Arc,
};

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	series::EncodedSeriesRow,
	shape::RowShape,
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	error::diagnostic::{
		catalog::{namespace_not_found, series_not_found, sumtype_variant_not_found},
		query::column_not_found,
	},
	interface::{
		catalog::{
			column::Column,
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			series::{Series, SeriesKey, SeriesPartitionMetadata, TimestampPrecision},
			storage::StorageId,
			sumtype::SumType,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedSeries},
	},
	internal_error,
	key::{
		any::TaggedKey,
		series::{PartitionedSeriesRowKey, SeriesRowKey},
	},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{expression::Expression, nodes::InsertSeriesNode};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{
		Value, datetime::DateTime, identity::IdentityId, partition::Partition, row_number::RowNumber,
		system_columns::SystemColumns,
	},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	context::SeriesTarget,
	returning::{decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image},
	shape::get_or_create_series_shape,
};
use crate::{
	Result,
	partition::resolve_partition,
	policy::PolicyEvaluator,
	transaction::operation::dictionary::DictionaryOperations,
	vm::{
		instruction::dml::{
			coerce::{coerce_series_row, series_key},
			time::resolve_time,
		},
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
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
	let (namespace, series) = resolve_insert_series_target(services, txn, &target)?;
	let mut metadata: HashMap<Partition, SeriesPartitionMetadata> = HashMap::new();
	let context = build_insert_series_query_context(
		services,
		&SeriesTarget {
			namespace: &namespace,
			series: &series,
		},
		&params,
		symbols,
		txn.identity(),
	);
	let mut input_node = compile(*input, txn, context.clone());

	let tag = series.tag.map(|tag_id| services.catalog.get_sumtype(txn, tag_id)).transpose()?;
	let key_column_name = series.key.column();
	let has_returning = returning.is_some();
	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = if has_returning {
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
		if let Some(unknown) = columns.names.iter().find(|name| {
			!(series.columns.iter().any(|c| c.name == name.text())
				|| (tag.is_some() && name.text() == "tag"))
		}) {
			return_error!(column_not_found(unknown.clone()));
		}
		for column in &series.columns {
			if let Some(input) = columns.column(&column.name) {
				input.data().check_digest_write(&column.constraint.get_type(), || {
					Fragment::internal(&column.name)
				})?;
			}
		}
		for row_idx in 0..columns.row_count() {
			insert_series_row(
				services,
				txn,
				&series,
				&mut metadata,
				&shape,
				&context,
				&columns,
				row_idx,
				key_column_name,
				tag.as_ref(),
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
	metadata_by_partition: &mut HashMap<Partition, SeriesPartitionMetadata>,
	shape: &RowShape,
	context: &QueryContext,
	columns: &Columns,
	row_idx: usize,
	key_column_name: &str,
	tag: Option<&SumType>,
	has_returning: bool,
	returned_rows: &mut Vec<(RowNumber, EncodedBytes)>,
	verified: &mut HashSet<Partition>,
) -> Result<()> {
	let values = coerce_series_row(series, columns, context, row_idx)?;
	let partition_values = series_partition_values(series, &values)?;
	let partition = if partition_values.is_empty() {
		Partition::default()
	} else {
		Partition::of(&partition_values)
	};
	let metadata = match metadata_by_partition.entry(partition) {
		Entry::Occupied(entry) => entry.into_mut(),
		Entry::Vacant(entry) => {
			let loaded =
				services.catalog.find_series_metadata(txn, series.id, partition)?.unwrap_or_default();
			entry.insert(loaded)
		}
	};

	let key_input = series
		.columns
		.iter()
		.zip(&values)
		.find(|(column, _)| column.name == key_column_name)
		.map(|(_, value)| value.clone())
		.unwrap_or_else(Value::none);
	let key_value = match series_key(series, &key_input)? {
		Some(key_value) => key_value,
		None => generate_series_key(services, &series.key, metadata),
	};
	let variant_tag = extract_variant_tag(columns, tag, row_idx)?;

	metadata.sequence_counter += 1;
	let sequence = metadata.sequence_counter;
	let key: TaggedKey = if partition_values.is_empty() {
		SeriesRowKey {
			storage: StorageId::series(series.id),
			variant_tag,
			key: key_value,
			sequence,
		}
		.into()
	} else {
		resolve_partition(txn, ObjectId::Series(series.id), partition, &partition_values, verified)?;
		PartitionedSeriesRowKey::new(StorageId::series(series.id), partition, variant_tag, key_value, sequence)
			.into()
	};

	let data_columns: Vec<_> = series.data_columns().collect();
	let data_values: Vec<Value> = series
		.columns
		.iter()
		.zip(values)
		.filter(|(column, _)| column.name != key_column_name)
		.map(|(_, value)| value)
		.collect();
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
	let row = build_encoded_series_row(services, series, shape, key_value, &encoded_values)?;

	let mut rows_buf = [EncodedSeriesRow::from(row).thaw()];
	SeriesRowInterceptor::pre_insert(txn, series, &mut rows_buf)?;
	let [row] = rows_buf;
	let row = row.freeze_bytes();
	txn.set(&key, row.clone())?;
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
	metadata_by_partition: HashMap<Partition, SeriesPartitionMetadata>,
	inserted_count: u64,
	returning: &Option<Vec<Expression>>,
	returned_rows: &[(RowNumber, EncodedBytes)],
) -> Result<Columns> {
	let now = services.runtime_context.clock.now();
	for (partition, mut metadata) in metadata_by_partition {
		metadata.last_write_at = now;
		services.catalog.update_series_metadata_txn(txn, series.id, partition, metadata)?;
	}

	if let Some(returning_exprs) = returning {
		let mut columns = decode_rows_to_columns(shape, returned_rows);
		decode_returning_dictionaries(services, txn, &series.columns, &mut columns)?;
		let columns = with_absent_pre_image(columns);
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	Ok(insert_series_result(namespace.name(), &series.name, inserted_count))
}

struct SeriesRowSnapshot<'a> {
	key_column_name: &'a str,
	key_value: u64,
	data_columns: &'a [&'a Column],
	data_values: &'a [Value],
	sequence: u64,
	row: &'a EncodedBytes,
}

#[inline]
fn resolve_insert_series_target(
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
fn series_partition_values(series: &Series, values: &[Value]) -> Result<Vec<Value>> {
	let mut partition_values = Vec::with_capacity(series.partition_by.len());
	for name in &series.partition_by {
		let idx = series.columns.iter().position(|c| c.name == *name).ok_or_else(|| {
			internal_error!("partition column {} missing from series {}", name, series.name)
		})?;
		partition_values.push(values[idx].clone());
	}
	Ok(partition_values)
}

#[inline]
fn build_insert_series_query_context(
	services: &Arc<Services>,
	target: &SeriesTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
	identity: IdentityId,
) -> Arc<QueryContext> {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(target.series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, target.series.clone());
	Arc::new(QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Series(resolved_series)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	})
}

#[inline]
fn generate_series_key(services: &Arc<Services>, key: &SeriesKey, metadata: &SeriesPartitionMetadata) -> u64 {
	match key {
		SeriesKey::DateTime {
			precision,
			..
		} => generate_timestamp(services, precision),
		SeriesKey::Integer {
			..
		} => metadata.newest_key + 1,
	}
}

#[inline]
fn extract_variant_tag(columns: &Columns, tag: Option<&SumType>, row_idx: usize) -> Result<Option<u8>> {
	let Some(sumtype) = tag else {
		return Ok(None);
	};
	let Some(tag_col) = columns.iter().find(|col| col.name().text() == "tag") else {
		return Ok(Some(0));
	};
	let value = tag_col.data().get_value(row_idx);
	match value {
		Value::None {
			..
		} => Ok(Some(0)),
		value => resolve_variant_tag(sumtype, &value, tag_col.name().with_text(value.to_string())).map(Some),
	}
}

pub(crate) fn resolve_variant_tag(sumtype: &SumType, value: &Value, fragment: Fragment) -> Result<u8> {
	match value.get_type().is_integer().then(|| value.to_usize()).flatten().and_then(|n| u8::try_from(n).ok()) {
		Some(tag) if sumtype.variants.iter().any(|v| v.tag == tag) => Ok(tag),
		_ => return_error!(sumtype_variant_not_found(fragment, &sumtype.name)),
	}
}

#[inline]
fn build_encoded_series_row(
	services: &Arc<Services>,
	series: &Series,
	shape: &RowShape,
	key_value: u64,
	data_values: &[Value],
) -> Result<EncodedBytes> {
	let key_value_encoded = series.key_from_u64(key_value);
	let mut row = shape.allocate_series();
	shape.set_value(&mut row, 0, &key_value_encoded);
	for (i, value) in data_values.iter().enumerate() {
		shape.set_value(&mut row, i + 1, value);
	}
	let now = services.runtime_context.clock.now();
	row.set_timestamps(now, now);
	if let Some(time) = resolve_time(&series.name, &series.columns, &series.time, shape, &row, now)? {
		row.set_time(time);
	}
	Ok(row.freeze_bytes())
}

fn track_series_insert_flow_change(txn: &mut Transaction<'_>, series: &Series, snapshot: &SeriesRowSnapshot<'_>) {
	let row_number = RowNumber::from(snapshot.sequence);
	let mut cols = Vec::with_capacity(1 + snapshot.data_columns.len());
	cols.push(ColumnWithName::new(
		Fragment::internal(snapshot.key_column_name),
		series.key_column_data(vec![snapshot.key_value]),
	));
	for (i, col_def) in snapshot.data_columns.iter().enumerate() {
		let mut data = ColumnBuilder::with_capacity(col_def.constraint.get_type(), 1);
		data.push_value(snapshot.data_values[i].clone());
		cols.push(ColumnWithName {
			name: Fragment::internal(&col_def.name),
			data: data.finish(),
		});
	}
	let post = Columns::with_system(
		cols,
		SystemColumns::new(
			vec![row_number],
			Vec::new(),
			vec![EncodedSeriesRow::view(snapshot.row).created_at()],
			vec![EncodedSeriesRow::view(snapshot.row).updated_at()],
			EncodedSeriesRow::view(snapshot.row).time().into_iter().collect(),
			Vec::new(),
		),
	);
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::insert(post)],
		changed_at: DateTime::default(),
	});
}

#[inline]
fn update_series_metadata_for_insert(metadata: &mut SeriesPartitionMetadata, key_value: u64) {
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
	metadata.dirty_from_key = metadata.dirty_from_key.min(key_value);
	metadata.dirty_to_key = metadata.dirty_to_key.max(key_value.saturating_add(1));
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
	let now = services.runtime_context.clock.now();
	match precision {
		TimestampPrecision::Second => now.to_secs(),
		TimestampPrecision::Millisecond => now.to_millis(),
		TimestampPrecision::Microsecond => now.to_micros(),
		TimestampPrecision::Nanosecond => now.to_nanos(),
	}
}
