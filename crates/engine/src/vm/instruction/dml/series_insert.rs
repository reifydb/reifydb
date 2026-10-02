// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	series::EncodedSeriesRowBuilder,
	shape::RowShape,
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	error::{
		CoreError,
		diagnostic::{
			catalog::{namespace_not_found, series_not_found, sumtype_variant_not_found},
			query::column_not_found,
		},
	},
	expression::Expression,
	interface::{
		catalog::{
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
	partition::partition_of,
	value::{
		batch::single_row,
		column::{builder::ColumnBuilder, write::check_digest_write},
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::InsertSeriesNode;
use reifydb_transaction::{
	interceptor::{WithInterceptors, series_row::SeriesRowInterceptor},
	transaction::Transaction,
};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		identity::IdentityId,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{column_view, user_columns},
		value_type::ValueType,
	},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	columns::{ColumnPipeline, Failure, input_views, intern_dictionary_columns},
	context::SeriesTarget,
	partition::partition_values,
	returning::{
		decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_absent_pre_image,
		with_series_stamps,
	},
	shape::get_or_create_series_shape,
};
use crate::{
	Result,
	partition::resolve_partition,
	policy::PolicyEvaluator,
	vm::{
		instruction::dml::{
			coerce::InputFragments,
			time::{EventColumn, populator_index, resolve_time},
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
) -> Result<RecordBatch> {
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
	let fragments = InputFragments::of(&input);
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

	let pipeline = ColumnPipeline {
		columns: &series.columns,
		sequences: None,
		series_key: Some(&series.key),
		fragments: &fragments,
		context: &context,
	};
	let key_index = series.columns.iter().position(|column| column.name == key_column_name);
	let partition_indices = series_partition_indices(&series)?;
	let populator = populator_index(&series.time, &shape);

	let mut mutable_context = (*context).clone();
	let mut verified: HashSet<Partition> = HashSet::new();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		enforce_series_write_policies(services, symbols, txn, &namespace, &series, &columns)?;
		if let Some((unknown, _)) = user_columns(&columns).find(|(field, _)| {
			!(series.columns.iter().any(|c| &c.name == field.name())
				|| (tag.is_some() && field.name() == "tag"))
		}) {
			return_error!(column_not_found(fragments.column(unknown.name())));
		}
		for column in &series.columns {
			if let Some(input) = column_view(&columns, &column.name)? {
				check_digest_write(&input, &column.constraint.get_type(), || {
					Fragment::internal(&column.name)
				})?;
			}
		}
		let rows = columns.num_rows();
		if rows == 0 {
			continue;
		}

		let tag_view = match tag {
			Some(_) => column_view(&columns, "tag")?,
			None => None,
		};
		let (tags, tag_failure) = variant_tags(&pipeline, tag.as_ref(), tag_view.as_ref(), rows);
		let inputs = input_views(&columns, &series.columns)?;
		let mut batches = [pipeline.cast_target_columns(&inputs, rows, tag_failure)?];
		let given_keys = match key_index {
			Some(index) => series.key.keys_to_u64(&batches[0].view(index)?),
			None => vec![None; rows],
		};
		let mut emitted = Vec::with_capacity(series.columns.len());
		for (index, column) in series.columns.iter().enumerate() {
			if Some(index) != key_index {
				let mut builder = ColumnBuilder::with_capacity(column.constraint.get_type(), rows);
				builder.append_values(&batches[0].view(index)?)?;
				emitted.push(builder.finish(&column.name));
			}
		}
		intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut batches)?;
		let [cast] = batches;
		let partition_rows = partition_values(&cast, &inputs, &partition_indices)?;

		let mut keys = Vec::with_capacity(rows);
		let mut storage_keys: Vec<TaggedKey> = Vec::with_capacity(rows);
		let mut row_numbers = Vec::with_capacity(rows);
		for ((given, variant_tag), partition_values) in given_keys.into_iter().zip(tags).zip(&partition_rows) {
			let partition = if partition_values.is_empty() {
				Partition::default()
			} else {
				partition_of(&series.columns, &series.partition_by, partition_values)
			};
			let partition_metadata = match metadata.entry(partition) {
				Entry::Occupied(entry) => entry.into_mut(),
				Entry::Vacant(entry) => {
					let loaded = services
						.catalog
						.find_series_metadata(txn, series.id, partition)?
						.unwrap_or_default();
					entry.insert(loaded)
				}
			};
			let key_value = match given {
				Some(key_value) => key_value,
				None => generate_series_key(services, &series, partition_metadata)?,
			};
			partition_metadata.sequence_counter += 1;
			let sequence = partition_metadata.sequence_counter;
			update_series_metadata_for_insert(partition_metadata, key_value);
			let storage_key: TaggedKey = if partition_values.is_empty() {
				SeriesRowKey {
					storage: StorageId::series(series.id),
					variant_tag,
					key: key_value,
					sequence,
				}
				.into()
			} else {
				resolve_partition(
					txn,
					ObjectId::Series(series.id),
					partition,
					partition_values,
					&mut verified,
				)?;
				PartitionedSeriesRowKey::new(
					StorageId::series(series.id),
					partition,
					variant_tag,
					key_value,
					sequence,
				)
				.into()
			};
			keys.push(key_value);
			storage_keys.push(storage_key);
			row_numbers.push(RowNumber::from(sequence));
		}

		let key_column = series.key_column_data(keys);
		let mut builders: Vec<EncodedSeriesRowBuilder> = (0..rows).map(|_| shape.allocate_series()).collect();
		let mut views = Vec::with_capacity(series.columns.len());
		views.push(ColumnView::try_from(&key_column)?);
		for index in 0..series.columns.len() {
			if Some(index) != key_index {
				views.push(cast.view(index)?);
			}
		}
		shape.write_columns(&mut builders, &views)?;
		let now = services.runtime_context.clock.now();
		let event = EventColumn::new(populator.map(|index| views[index].clone()));
		for (index, builder) in builders.iter_mut().enumerate() {
			builder.set_timestamps(now, now);
			if let Some(time) = event.at(index).map_or_else(
				|| resolve_time(&series.name, &series.time, &shape, builder, now),
				|time| Ok(Some(time)),
			)? {
				builder.set_time(time);
			}
		}

		if !txn.series_row_pre_insert_interceptors().is_empty() {
			SeriesRowInterceptor::pre_insert(txn, &series, &mut builders)?;
		}
		let written: Vec<EncodedBytes> = builders.into_iter().map(|builder| builder.freeze_bytes()).collect();
		for (storage_key, row) in storage_keys.iter().zip(&written) {
			txn.set(storage_key, row.clone())?;
		}
		if !txn.series_row_post_insert_interceptors().is_empty() {
			SeriesRowInterceptor::post_insert(txn, &series, &written)?;
		}

		if has_returning {
			returned_rows.extend(row_numbers.iter().copied().zip(written.iter().cloned()));
		}
		inserted_count += rows as u64;
		let mut changed = Vec::with_capacity(1 + emitted.len());
		changed.push(key_column);
		changed.extend(emitted);
		track_series_insert_flow_change(txn, &series, changed, &row_numbers, &written)?;
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
	columns: &RecordBatch,
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
) -> Result<RecordBatch> {
	let now = services.runtime_context.clock.now();
	for (partition, mut metadata) in metadata_by_partition {
		metadata.last_write_at = now;
		services.catalog.update_series_metadata_txn(txn, series.id, partition, metadata)?;
	}

	if let Some(returning_exprs) = returning {
		let columns = decode_rows_to_columns(shape, returned_rows)?;
		let columns = decode_returning_dictionaries(services, txn, &series.columns, columns)?;
		let columns = with_absent_pre_image(columns)?;
		return evaluate_returning(services, symbols, returning_exprs, columns, txn.identity());
	}
	insert_series_result(namespace.name(), &series.name, inserted_count)
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

fn series_partition_indices(series: &Series) -> Result<Vec<usize>> {
	series.partition_by
		.iter()
		.map(|name| {
			series.columns.iter().position(|c| c.name == *name).ok_or_else(|| {
				internal_error!("partition column {} missing from series {}", name, series.name)
			})
		})
		.collect()
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
fn generate_series_key(services: &Arc<Services>, series: &Series, metadata: &SeriesPartitionMetadata) -> Result<u64> {
	match &series.key {
		SeriesKey::DateTime {
			precision,
			..
		} => Ok(generate_timestamp(services, precision)),
		SeriesKey::Integer {
			..
		} => {
			let Some(key_type) = series.key_column_type() else {
				return Err(internal_error!("series {} has no key column type", series.name));
			};
			let max = match key_type {
				ValueType::Int1 => i8::MAX as u64,
				ValueType::Int2 => i16::MAX as u64,
				ValueType::Int4 => i32::MAX as u64,
				ValueType::Int8 => i64::MAX as u64,
				ValueType::Uint1 => u8::MAX as u64,
				ValueType::Uint2 => u16::MAX as u64,
				ValueType::Uint4 => u32::MAX as u64,
				_ => u64::MAX,
			};
			match metadata.newest_key.checked_add(1) {
				Some(next) if next <= max => Ok(next),
				_ => Err(CoreError::SequenceExhausted {
					value_type: key_type,
				}
				.into()),
			}
		}
	}
}

fn variant_tags(
	pipeline: &ColumnPipeline<'_>,
	tag: Option<&SumType>,
	view: Option<&ColumnView<'_>>,
	rows: usize,
) -> (Vec<Option<u8>>, Option<Failure>) {
	let Some(sumtype) = tag else {
		return (vec![None; rows], None);
	};
	let Some(view) = view else {
		return (vec![Some(0); rows], None);
	};
	let mut tags = Vec::with_capacity(rows);
	for row in 0..rows {
		match view.get_value(row) {
			Value::None {
				..
			} => tags.push(Some(0)),
			value => match resolve_variant_tag(sumtype, &value, Fragment::internal(value.to_string())) {
				Ok(variant_tag) => tags.push(Some(variant_tag)),
				Err(error) => {
					tags.resize(rows, None);
					return (tags, Some(Failure::after_key(pipeline, row, error)));
				}
			},
		}
	}
	(tags, None)
}

pub(crate) fn resolve_variant_tag(sumtype: &SumType, value: &Value, fragment: Fragment) -> Result<u8> {
	match value.get_type().is_integer().then(|| value.to_usize()).flatten().and_then(|n| u8::try_from(n).ok()) {
		Some(tag) if sumtype.variants.iter().any(|v| v.tag == tag) => Ok(tag),
		_ => return_error!(sumtype_variant_not_found(fragment, &sumtype.name)),
	}
}

fn track_series_insert_flow_change(
	txn: &mut Transaction<'_>,
	series: &Series,
	columns: Vec<(FieldRef, ArrayRef)>,
	row_numbers: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<()> {
	if rows.is_empty() {
		return Ok(());
	}
	let post = with_series_stamps(columns, row_numbers, rows)?;
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::insert(post)],
		changed_at: DateTime::default(),
	});
	Ok(())
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
fn insert_series_result(namespace: &str, series: &str, inserted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("inserted", Value::Uint8(inserted)),
	])
}

fn generate_timestamp(services: &Services, precision: &TimestampPrecision) -> u64 {
	let now = services.runtime_context.clock.now();
	let scaled = match precision {
		TimestampPrecision::Second => now.to_secs(),
		TimestampPrecision::Millisecond => now.to_millis(),
		TimestampPrecision::Microsecond => now.to_micros(),
		TimestampPrecision::Nanosecond => now.to_nanos(),
	};
	u64::try_from(scaled).expect("clock is never before the Unix epoch")
}
