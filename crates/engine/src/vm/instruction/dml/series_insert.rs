// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::row::EncodedRow,
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			policy::{DataOp, PolicyTargetType},
			series::{SeriesKey, TimestampPrecision},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::nodes::InsertSeriesNode;
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::{
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
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let series_name = plan.target.name();
	let Some(series) = services.catalog.find_series_by_name(txn, namespace.id(), series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	// Get current metadata
	let Some(mut metadata) = services.catalog.find_series_metadata(txn, series.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series.tag.is_some();
	let key = &series.key;
	let key_column_name = series.key.column();

	// Create resolved source for the series
	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let series_ident = Fragment::internal(series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series.clone());
	let resolved_source = Some(ResolvedShape::Series(resolved_series));

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	let mut inserted_count = 0u64;
	let mut returned_rows: Vec<(RowNumber, EncodedRow)> = if plan.returning.is_some() {
		Vec::with_capacity(16)
	} else {
		Vec::new()
	};

	// Initialize the operator before execution
	input_node.initialize(txn, &execution_context)?;

	// Create shape for series encoding
	let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;

	// Process all input batches
	let mut mutable_context = (*execution_context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		// Enforce write policies before processing rows
		PolicyEvaluator::new(services, symbols).enforce_write_policies(
			txn,
			namespace_name,
			series_name,
			DataOp::Insert,
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			// Extract or generate key value
			let key_value: u64 = if let Some(key_col) =
				columns.iter().find(|col| col.name().text() == key_column_name)
			{
				match series.key_to_u64(key_col.data().get_value(row_idx)) {
					Some(v) => v,
					None => match key {
						SeriesKey::DateTime {
							precision,
							..
						} => generate_timestamp(services, precision),
						SeriesKey::Integer {
							..
						} => metadata.newest_key + 1,
					},
				}
			} else {
				match key {
					SeriesKey::DateTime {
						precision,
						..
					} => generate_timestamp(services, precision),
					SeriesKey::Integer {
						..
					} => metadata.newest_key + 1,
				}
			};

			// Extract optional tag
			let variant_tag: Option<u8> = if has_tag {
				if let Some(tag_col) = columns.iter().find(|col| col.name().text() == "tag") {
					match tag_col.data().get_value(row_idx) {
						Value::Uint1(t) => Some(t),
						Value::Int1(t) => Some(t as u8),
						_ => Some(0),
					}
				} else {
					Some(0)
				}
			} else {
				None
			};

			// Allocate sequence number
			metadata.sequence_counter += 1;
			let sequence = metadata.sequence_counter;

			// Build key
			let row_key = SeriesRowKey {
				series: series.id,
				variant_tag,
				key: key_value,
				sequence,
			};
			let encoded_key = row_key.encode();

			// Collect data column values (excluding key column)
			let data_columns: Vec<_> = series.data_columns().collect();
			let mut data_values = Vec::with_capacity(data_columns.len());
			for col_def in &data_columns {
				let value = if let Some(input_col) =
					columns.iter().find(|c| c.name().text() == col_def.name)
				{
					input_col.data().get_value(row_idx)
				} else {
					Value::none()
				};
				data_values.push(value);
			}

			// Encode using shape (key at index 0, data columns at index 1+)
			let key_value_encoded = series.key_from_u64(key_value);
			let mut row = shape.allocate();
			shape.set_value(&mut row, 0, &key_value_encoded);
			for (i, value) in data_values.iter().enumerate() {
				shape.set_value(&mut row, i + 1, value);
			}

			let now_nanos = services.runtime_context.clock.now_nanos();
			row.set_timestamps(now_nanos, now_nanos);

			let row = SeriesRowInterceptor::pre_insert(txn, &series, row)?;
			txn.set(&encoded_key, row.clone())?;
			SeriesRowInterceptor::post_insert(txn, &series, &row)?;

			if plan.returning.is_some() {
				returned_rows.push((RowNumber::from(sequence), row.clone()));
			}

			// Track flow change for transactional/deferred view processing
			{
				let row_number = RowNumber::from(sequence);
				let mut cols = Vec::with_capacity(1 + data_columns.len());
				cols.push(Column {
					name: Fragment::internal(key_column_name),
					data: series.key_column_data(vec![key_value]),
				});
				for (i, col_def) in data_columns.iter().enumerate() {
					let mut data = ColumnData::with_capacity(col_def.constraint.get_type(), 1);
					data.push_value(data_values[i].clone());
					cols.push(Column {
						name: Fragment::internal(&col_def.name),
						data,
					});
				}
				let post = Columns {
					row_numbers: CowVec::new(vec![row_number]),
					created_at: CowVec::new(vec![DateTime::from_nanos(row.created_at_nanos())]),
					updated_at: CowVec::new(vec![DateTime::from_nanos(row.updated_at_nanos())]),
					columns: CowVec::new(cols),
				};
				txn.track_flow_change(Change {
					origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Insert {
						post,
					}],
					changed_at: DateTime::default(),
				});
			}

			// Update metadata
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
			inserted_count += 1;
		}
	}

	// Save updated metadata
	services.catalog.update_series_metadata_txn(txn, metadata)?;

	// If RETURNING clause is present, evaluate expressions against inserted rows
	if let Some(returning_exprs) = &plan.returning {
		let columns = decode_rows_to_columns(&shape, &returned_rows);
		return evaluate_returning(services, symbols, returning_exprs, columns);
	}

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("series", Value::Utf8(series.name)),
		("inserted", Value::Uint8(inserted_count)),
	]))
}

fn generate_timestamp(services: &Services, precision: &TimestampPrecision) -> u64 {
	match precision {
		TimestampPrecision::Second => services.runtime_context.clock.now_secs(),
		TimestampPrecision::Millisecond => services.runtime_context.clock.now_millis(),
		TimestampPrecision::Microsecond => services.runtime_context.clock.now_micros(),
		TimestampPrecision::Nanosecond => services.runtime_context.clock.now_nanos(),
	}
}
