// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	series::{EncodedSeriesRow, EncodedSeriesRowBuilder},
	shape::RowShape,
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	error::diagnostic::{
		catalog::{namespace_not_found, series_not_found},
		query::column_not_found,
	},
	expression::Expression,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			series::Series,
			storage::StorageId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedSeries},
	},
	internal_error,
	key::{
		any::TaggedKey,
		series::{PartitionedSeriesRowKey, SeriesRowKey},
	},
	partition::{PartitionError, partition_of, partition_values},
	value::{
		batch::{decode_cells, single_row, take_rows},
		column::builder::ColumnBuilder,
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{nodes::UpdateSeriesNode, query::QueryPlan};
use reifydb_transaction::{
	interceptor::{WithInterceptors, series_row::SeriesRowInterceptor},
	transaction::Transaction,
};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	return_error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		datetime::DateTime,
		identity::IdentityId,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{column_view, partitions, row_numbers, user_columns},
	},
};
use smallvec::smallvec;
use tracing::instrument;

use super::{
	columns::{ColumnPipeline, Failure, input_views, intern_dictionary_columns},
	context::SeriesTarget,
	returning::{
		decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image,
		with_series_stamps,
	},
};
use crate::{
	Result,
	error::EngineError,
	policy::PolicyEvaluator,
	vm::{
		instruction::dml::{
			coerce::{InputFragments, series_key},
			shape::get_or_create_series_shape,
			time::resolve_time_for_update,
		},
		services::Services,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, query_budget},
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
) -> Result<RecordBatch> {
	let UpdateSeriesNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, series) = resolve_update_series_target(services, txn, &target)?;
	reject_series_row_key_assignment(&namespace, &series, &input)?;
	let target_data = SeriesTarget {
		namespace: &namespace,
		series: &series,
	};
	let context = build_update_series_query_context(services, &target_data, &params, symbols, txn.identity());
	let fragments = InputFragments::of(&input);
	let mut input_node = compile(*input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let has_tag = series.tag.is_some();
	let mut updated_count = 0u64;
	let has_returning = returning.is_some();
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let mut pre_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let pipeline = ColumnPipeline {
		columns: &series.columns,
		sequences: None,
		series_key: Some(&series.key),
		fragments: &fragments,
		context: &context,
	};
	let mut statement_shape: Option<RowShape> = None;

	let mut mutable_context = context.clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.num_rows();
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
		if let Some((unknown, _)) = user_columns(&columns).find(|(field, _)| {
			!(series.columns.iter().any(|c| &c.name == field.name()) || (has_tag && field.name() == "tag"))
		}) {
			return_error!(column_not_found(fragments.column(unknown.name())));
		}

		let row_numbers = row_numbers(&columns)?;
		let partitioned = !series.partition_by.is_empty();
		let sidecar_partitions = partitions(&columns)?;
		if partitioned && sidecar_partitions.len() != row_count {
			return Err(EngineError::MissingPartitionAddress {
				object: ObjectId::series(series.id),
				operation: "UPDATE",
			}
			.into());
		}
		let (converted, key_failure) = series_update_keys(&series, &columns)?;
		let tags = series_update_tags(&columns, has_tag, row_count)?;
		let inputs = input_views(&columns, &series.columns)?;
		let mut batches = [pipeline.cast_target_columns(&inputs, row_count, key_failure)?];
		let keys: Vec<u64> = converted
			.into_iter()
			.map(|key| key.expect("the pipeline fails on the first key that does not convert"))
			.collect();
		let shape = match &statement_shape {
			Some(shape) => shape.clone(),
			None => {
				let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;
				statement_shape = Some(shape.clone());
				shape
			}
		};
		intern_dictionary_columns(&services.catalog, txn, pipeline.columns, pipeline.series_key, &mut batches)?;
		let [cast] = batches;

		let storage_keys: Vec<TaggedKey> = (0..row_count)
			.map(|row| {
				let sequence = u64::from(row_numbers[row]);
				if partitioned {
					PartitionedSeriesRowKey::new(
						StorageId::series(series.id),
						sidecar_partitions[row],
						tags[row],
						keys[row],
						sequence,
					)
					.into()
				} else {
					SeriesRowKey {
						storage: StorageId::series(series.id),
						variant_tag: tags[row],
						key: keys[row],
						sequence,
					}
					.into()
				}
			})
			.collect();
		let mut builders: Vec<EncodedSeriesRowBuilder> =
			(0..row_count).map(|_| shape.allocate_series()).collect();
		{
			let key_column = series.key_column_data(keys.clone());
			let mut views = Vec::with_capacity(series.columns.len());
			views.push(ColumnView::try_from(&key_column)?);
			for (index, column) in series.columns.iter().enumerate() {
				if column.name != series.key.column() {
					views.push(cast.view(index)?);
				}
			}
			shape.write_columns(&mut builders, &views)?;
		}
		enforce_old_row_policies(services, symbols, txn, &target_data, &storage_keys, row_numbers, &shape)?;

		let mut found = Vec::with_capacity(row_count);
		let mut found_builders = Vec::with_capacity(row_count);
		let mut pres = Vec::with_capacity(row_count);
		for (row, mut builder) in builders.into_iter().enumerate() {
			let Some(pre) = txn.get(&storage_keys[row])? else {
				continue;
			};
			let pre = pre.bytes;
			let old_created_at = EncodedSeriesRow::view(&pre).created_at();
			let old_time = EncodedSeriesRow::view(&pre).time();
			let now = services.runtime_context.clock.now();
			builder.set_timestamps(old_created_at, now);
			if let Some(time) = resolve_time_for_update(
				&series.name,
				&series.columns,
				&series.time,
				&shape,
				builder.as_slice(),
				old_time,
			)? {
				builder.set_time(time);
			}
			found.push(row);
			found_builders.push(builder);
			pres.push(pre);
		}

		if !found_builders.is_empty() && !txn.series_row_pre_update_interceptors().is_empty() {
			SeriesRowInterceptor::pre_update(txn, &series, &mut found_builders)?;
		}
		let posts: Vec<EncodedBytes> =
			found_builders.into_iter().map(|builder| builder.freeze_bytes()).collect();
		for (&row, post) in found.iter().zip(&posts) {
			let storage_key = &storage_keys[row];
			if partitioned && series_partition_of_bytes(&series, &shape, post) != sidecar_partitions[row] {
				return Err(PartitionError::ImmutablePartitionColumn {
					object: ObjectId::series(series.id),
				}
				.into());
			}
			if txn.get_committed(storage_key)?.is_some() {
				txn.mark_preexisting(storage_key)?;
			}
			txn.set(storage_key, post.clone())?;
		}
		if !posts.is_empty() && !txn.series_row_post_update_interceptors().is_empty() {
			SeriesRowInterceptor::post_update(txn, &series, &posts, &pres)?;
		}

		let found_numbers: Vec<RowNumber> = found.iter().map(|&row| row_numbers[row]).collect();
		if has_returning {
			returned_rows.extend(found_numbers.iter().copied().zip(posts.iter().cloned()));
			pre_rows.extend(found_numbers.iter().copied().zip(pres.iter().cloned()));
		}
		updated_count += posts.len() as u64;
		let found_keys: Vec<u64> = found.iter().map(|&row| keys[row]).collect();
		track_series_update_flow_change(
			txn,
			&series,
			&shape,
			&columns,
			SeriesUpdatedRows {
				keys: found_keys,
				row_numbers: found_numbers,
				row_indices: found,
				pres,
				posts,
			},
		)?;
	}

	if let Some(returning_exprs) = &returning {
		let shape = match statement_shape {
			Some(shape) => shape,
			None => get_or_create_series_shape(&services.catalog, &series, txn)?,
		};
		let cols = decode_rows_to_columns(&shape, &returned_rows)?;
		let cols = decode_returning_dictionaries(services, txn, &series.columns, cols)?;
		let pre_cols = decode_rows_to_columns(&shape, &pre_rows)?;
		let pre_cols = decode_returning_dictionaries(services, txn, &series.columns, pre_cols)?;
		let cols = with_pre_image(cols, &pre_cols)?;
		return evaluate_returning(services, symbols, returning_exprs, cols, txn.identity());
	}
	update_series_result(namespace.name(), &series.name, updated_count)
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

fn reject_series_row_key_assignment(namespace: &Namespace, series: &Series, input: &QueryPlan) -> Result<()> {
	let QueryPlan::Patch(patch) = input else {
		return Err(internal_error!("update of series {} has no patch at the top of its input", series.name));
	};
	let key_column = series.key.column();
	for assignment in &patch.assignments {
		let Expression::Alias(alias) = assignment else {
			continue;
		};
		let name = alias.alias.name();
		if series.tag.is_some() && name == "tag" {
			return Err(EngineError::SeriesTagImmutable {
				series: format!("{}::{}", namespace.name(), series.name),
				fragment: alias.expression.full_fragment_owned(),
			}
			.into());
		}
		if name == key_column {
			return Err(EngineError::SeriesKeyImmutable {
				series: format!("{}::{}", namespace.name(), series.name),
				column: key_column.to_string(),
				fragment: alias.expression.full_fragment_owned(),
			}
			.into());
		}
	}
	Ok(())
}

#[inline]
fn build_update_series_query_context(
	services: &Arc<Services>,
	target: &SeriesTarget<'_>,
	params: &Params,
	symbols: &SymbolTable,
	identity: IdentityId,
) -> QueryContext {
	let namespace_ident = Fragment::internal(target.namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, target.namespace.clone());
	let series_ident = Fragment::internal(target.series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, target.series.clone());
	QueryContext {
		services: services.clone(),
		source: Some(ResolvedObject::Series(resolved_series)),
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params: params.clone(),
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	}
}

fn enforce_old_row_policies(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	txn: &mut Transaction<'_>,
	target: &SeriesTarget<'_>,
	keys: &[TaggedKey],
	row_numbers: &[RowNumber],
	shape: &RowShape,
) -> Result<()> {
	if txn.identity().is_privileged() {
		return Ok(());
	}
	let series = target.series;
	let mut old_rows: Vec<(RowNumber, EncodedBytes)> = Vec::with_capacity(keys.len());
	for (key, &row_number) in keys.iter().zip(row_numbers) {
		if let Some(old) = txn.get(key)? {
			old_rows.push((row_number, old.bytes));
		}
	}
	let old_columns = decode_rows_to_columns(shape, &old_rows)?;
	let old_columns = decode_returning_dictionaries(services, txn, &series.columns, old_columns)?;
	PolicyEvaluator::new(services, symbols).enforce_write_policies(
		txn,
		target.namespace.name(),
		&series.name,
		DataOp::Update,
		&old_columns,
		PolicyTargetType::Series,
	)
}

#[inline]
fn series_partition_of_bytes(series: &Series, shape: &RowShape, bytes: &EncodedBytes) -> Partition {
	let key_column = series.key.column();
	let indices: Vec<usize> = series
		.partition_by
		.iter()
		.map(|name| {
			if name == key_column {
				0
			} else {
				1 + series
					.data_columns()
					.position(|c| c.name == *name)
					.expect("partition column must exist (validated during planning)")
			}
		})
		.collect();
	partition_of(&series.columns, &series.partition_by, &partition_values(shape, bytes, &indices))
}

fn series_update_keys(series: &Series, columns: &RecordBatch) -> Result<(Vec<Option<u64>>, Option<Failure>)> {
	let key_column = series.key.column();
	let view = column_view(columns, key_column)?.ok_or_else(|| {
		internal_error!("update of series {} has no key column {} in its input", series.name, key_column)
	})?;
	let keys = series.key.keys_to_u64(&view);
	let failure = keys.iter().position(Option::is_none).map(|row| {
		let error = match series_key(series, &view.get_value(row)) {
			Err(error) => error,
			Ok(_) => internal_error!("update of series {} reads a row without a key", series.name),
		};
		Failure::before_columns(row, error)
	});
	Ok((keys, failure))
}

fn series_update_tags(columns: &RecordBatch, has_tag: bool, rows: usize) -> Result<Vec<Option<u8>>> {
	if !has_tag {
		return Ok(vec![None; rows]);
	}
	Ok(match column_view(columns, "tag")? {
		Some(view) => match &view.data {
			ViewData::Uint1(array) => {
				(0..rows).map(|row| (!view.none_at(row)).then(|| array.value(row))).collect()
			}
			_ => vec![None; rows],
		},
		None => vec![None; rows],
	})
}

struct SeriesUpdatedRows {
	keys: Vec<u64>,
	row_numbers: Vec<RowNumber>,
	row_indices: Vec<usize>,
	pres: Vec<EncodedBytes>,
	posts: Vec<EncodedBytes>,
}

fn track_series_update_flow_change(
	txn: &mut Transaction<'_>,
	series: &Series,
	shape: &RowShape,
	columns: &RecordBatch,
	updated: SeriesUpdatedRows,
) -> Result<()> {
	if updated.posts.is_empty() {
		return Ok(());
	}
	let mut pre_columns = Vec::with_capacity(1 + series.columns.len());
	pre_columns.push(series.key_column_data(updated.keys.clone()));
	let fields = shape.fields();
	for (i, column) in series.data_columns().enumerate() {
		let mut builder = ColumnBuilder::with_capacity(fields[i + 1].constraint.get_type(), updated.pres.len());
		decode_cells(&mut builder, &column.name, shape, i + 1, &updated.pres)?;
		pre_columns.push(builder.finish(&column.name));
	}

	let mut post_columns = Vec::with_capacity(1 + series.columns.len());
	post_columns.push(series.key_column_data(updated.keys));
	let taken = take_rows(columns, &updated.row_indices)?;
	for (field, array) in user_columns(&taken) {
		if field.name() != series.key.column() && field.name() != "tag" {
			let view = ColumnView::try_from((array, field.as_ref()))?;
			let mut builder = ColumnBuilder::with_capacity(view.get_type(), updated.row_indices.len());
			builder.append_values(&view)?;
			post_columns.push(builder.finish(field.name()));
		}
	}

	let pre = with_series_stamps(pre_columns, &updated.row_numbers, &updated.pres)?;
	let post = with_series_stamps(post_columns, &updated.row_numbers, &updated.posts)?;
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::update(pre, post)],
		changed_at: DateTime::default(),
	});
	Ok(())
}

#[inline]
fn update_series_result(namespace: &str, series: &str, updated: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("updated", Value::Uint8(updated)),
	])
}
