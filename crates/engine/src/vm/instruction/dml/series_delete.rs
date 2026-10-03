// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			namespace::Namespace,
			object::ObjectId,
			policy::{DataOp, PolicyTargetType},
			series::Series,
			storage::StorageId,
		},
		resolved::{ResolvedNamespace, ResolvedObject, ResolvedSeries},
	},
	internal_error,
	key::{
		any::TaggedKey,
		series::{PartitionedSeriesRowKey, SeriesRowKey},
	},
	value::{
		batch::{single_row, take_rows},
		column::builder::ColumnBuilder,
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::{nodes::DeleteSeriesNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	reifydb_assertions, return_error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		identity::IdentityId,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{column_view, partitions, row_numbers, user_columns},
	},
};
use tracing::instrument;

use super::{
	context::{SeriesTarget, WriteExecCtx},
	returning::{
		decode_returning_dictionaries, decode_rows_to_columns, evaluate_returning, with_pre_image,
		with_series_stamps,
	},
};
use crate::{
	Result,
	error::EngineError,
	policy::PolicyEvaluator,
	transaction::operation::series::{
		SeriesDeleteTally, apply_series_metadata_after_delete, emit_series_remove_change, remove_series_rows,
	},
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
) -> Result<RecordBatch> {
	let DeleteSeriesNode {
		input,
		target,
		returning,
	} = plan;
	let (namespace, series) = resolve_delete_series_target(services, txn, &target)?;
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
	let (deleted_by_partition, returned_rows) =
		run_series_delete_with_input(&exec, txn, *input_plan, &target_data, &params, has_tag, has_returning)?;

	let deleted_count: u64 = deleted_by_partition.values().map(|tally| tally.count).sum();
	for (partition, tally) in deleted_by_partition {
		let Some(mut metadata) = services.catalog.find_series_metadata(txn, series.id, partition)? else {
			continue;
		};
		apply_series_metadata_after_delete(&mut metadata, &tally);
		services.catalog.update_series_metadata_txn(txn, series.id, partition, metadata)?;
	}

	if let Some(returning_exprs) = &returning {
		let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;
		let cols = decode_rows_to_columns(&shape, &returned_rows)?;
		let cols = decode_returning_dictionaries(services, txn, &series.columns, cols)?;
		let cols = with_pre_image(cols.clone(), &cols)?;
		return evaluate_returning(services, symbols, returning_exprs, cols, txn.identity());
	}
	delete_series_result(namespace.name(), &series.name, deleted_count)
}

#[inline]
fn resolve_delete_series_target(
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

type SeriesDeleteOutcome = (HashMap<Partition, SeriesDeleteTally>, Vec<(RowNumber, EncodedBytes)>);

fn run_series_delete_with_input(
	exec: &WriteExecCtx<'_>,
	txn: &mut Transaction<'_>,
	input_plan: QueryPlan,
	target: &SeriesTarget<'_>,
	params: &Params,
	has_tag: bool,
	has_returning: bool,
) -> Result<SeriesDeleteOutcome> {
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
) -> Result<SeriesDeleteOutcome> {
	let series = target.series;
	let mut deleted_by_partition: HashMap<Partition, SeriesDeleteTally> = HashMap::new();
	let mut returned_rows: Vec<(RowNumber, EncodedBytes)> = Vec::new();
	let mut mutable_context = context.clone();

	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.num_rows();
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

		let row_numbers = row_numbers(&columns)?;
		reifydb_assertions! {
			let row_numbers_len = row_numbers.len();
			assert!(
				row_numbers_len == row_count,
				"series delete loop indexes row_numbers[0..row_count] but row_numbers.len()={row_numbers_len} != row_count={row_count}; \
				 a row batch without parallel row_numbers would panic out of bounds while building the delete key sequence"
			);
		}
		let partitioned = !series.partition_by.is_empty();
		let sidecar_partitions = partitions(&columns)?;
		if partitioned && sidecar_partitions.len() != row_count {
			return Err(EngineError::MissingPartitionAddress {
				object: ObjectId::series(series.id),
				operation: "DELETE",
			}
			.into());
		}
		let keys = series_delete_keys(series, &columns)?;
		let tags = series_delete_tags(&columns, has_tag, row_count)?;
		let mut removed = SeriesRemovedRows::default();
		let mut removals: Vec<(TaggedKey, EncodedBytes, bool)> = Vec::new();
		for (row_idx, &row_number) in row_numbers.iter().enumerate() {
			let sequence = u64::from(row_number);
			let key_value = keys[row_idx];
			let variant_tag = tags[row_idx];
			let partition = if partitioned {
				sidecar_partitions[row_idx]
			} else {
				Partition::default()
			};
			let key: TaggedKey = if partitioned {
				PartitionedSeriesRowKey::new(
					StorageId::series(series.id),
					partition,
					variant_tag,
					key_value,
					sequence,
				)
				.into()
			} else {
				SeriesRowKey {
					storage: StorageId::series(series.id),
					variant_tag,
					key: key_value,
					sequence,
				}
				.into()
			};

			let Some(pre_entry) = txn.get(&key)? else {
				continue;
			};
			let encoded_bytes = pre_entry.bytes;
			let row_number = RowNumber::from(sequence);

			let committed = txn.get_committed(&key)?.map(|v| v.bytes);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| encoded_bytes.clone());

			if has_returning {
				returned_rows.push((row_number, encoded_bytes));
			}
			deleted_by_partition.entry(partition).or_default().record(key_value);
			removed.key_values.push(key_value);
			removed.row_numbers.push(row_number);
			removed.pres.push(pre_for_cdc.clone());
			removed.row_indices.push(row_idx);
			removals.push((key, pre_for_cdc, committed.is_some()));
		}
		remove_series_rows(txn, series, &removed.row_numbers, &removals)?;
		if !removed.pres.is_empty() {
			let pre = build_series_delete_pre_columns_from_input(series, &columns, removed)?;
			emit_series_remove_change(txn, series, pre);
		}
	}

	Ok((deleted_by_partition, returned_rows))
}

fn series_delete_keys(series: &Series, columns: &RecordBatch) -> Result<Vec<u64>> {
	let key_column = series.key.column();
	let view = column_view(columns, key_column)?.ok_or_else(|| {
		internal_error!("delete of series {} has no key column {} in its input", series.name, key_column)
	})?;
	series.key
		.keys_to_u64(&view)
		.into_iter()
		.map(|key| {
			key.ok_or_else(|| internal_error!("delete of series {} reads a row without a key", series.name))
		})
		.collect()
}

fn series_delete_tags(columns: &RecordBatch, has_tag: bool, rows: usize) -> Result<Vec<Option<u8>>> {
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

#[derive(Default)]
struct SeriesRemovedRows {
	key_values: Vec<u64>,
	row_numbers: Vec<RowNumber>,
	pres: Vec<EncodedBytes>,
	row_indices: Vec<usize>,
}

fn build_series_delete_pre_columns_from_input(
	series: &Series,
	columns: &RecordBatch,
	removed: SeriesRemovedRows,
) -> Result<RecordBatch> {
	let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
	pre_col_vec.push(series.key_column_data(removed.key_values));
	let taken = take_rows(columns, &removed.row_indices)?;
	for (field, array) in user_columns(&taken) {
		if field.name() != series.key.column() && field.name() != "tag" {
			let view = ColumnView::try_from((array, field.as_ref()))?;
			let mut builder = ColumnBuilder::with_capacity(view.get_type(), removed.row_indices.len());
			builder.append_values(&view)?;
			pre_col_vec.push(builder.finish(field.name()));
		}
	}
	with_series_stamps(pre_col_vec, &removed.row_numbers, &removed.pres)
}

#[inline]
fn delete_series_result(namespace: &str, series: &str, deleted: u64) -> Result<RecordBatch> {
	single_row([
		("namespace", Value::Utf8(namespace.to_string())),
		("series", Value::Utf8(series.to_string())),
		("deleted", Value::Uint8(deleted)),
	])
}

#[cfg(test)]
mod tests {
	use arrow_array::RecordBatch;
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			column::{Column, ColumnIndex},
			id::{ColumnId, NamespaceId, SeriesId},
			series::{Series, SeriesKey},
		},
		value::{batch::batch, column::builder::ColumnBuilder},
	};
	use reifydb_value::value::{Value, constraint::TypeConstraint, value_type::ValueType};

	use super::series_delete_keys;

	fn series() -> Series {
		let column = |index: u8, name: &str| Column {
			id: ColumnId(index as u64 + 1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ValueType::Int4),
			properties: vec![],
			index: ColumnIndex(index),
			auto_increment: false,
			dictionary_id: None,
		};
		Series {
			id: SeriesId(1),
			namespace: NamespaceId(1),
			name: "s".to_string(),
			columns: vec![column(0, "k"), column(1, "v")],
			tag: None,
			key: SeriesKey::Integer {
				column: "k".to_string(),
			},
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::None,
		}
	}

	fn input(name: &str, ty: ValueType, values: Vec<Value>) -> RecordBatch {
		let mut builder = ColumnBuilder::with_capacity(ty, values.len());
		for value in values {
			builder.push_value(value);
		}
		batch(vec![builder.finish(name)]).unwrap()
	}

	#[test]
	fn a_delete_key_that_does_not_convert_is_an_error() {
		// A bad key must fail the delete, otherwise it reads as key 0 and the row silently stays.
		let series = series();
		let optional = ValueType::Option(Box::new(ValueType::Int4));

		let converted =
			series_delete_keys(&series, &input("k", ValueType::Int4, vec![Value::Int4(1), Value::Int4(7)]));

		assert_eq!(converted.unwrap(), vec![1, 7]);
		for (label, columns) in [
			("a missing key column", input("v", ValueType::Int4, vec![Value::Int4(1)])),
			("a none key", input("k", optional, vec![Value::Int4(1), Value::none_of(ValueType::Int4)])),
			("a negative key", input("k", ValueType::Int4, vec![Value::Int4(-5)])),
		] {
			let error = series_delete_keys(&series, &columns).expect_err(label);
			assert_eq!(error.0.code, "INTERNAL_ERROR", "{label}");
		}
	}
}
