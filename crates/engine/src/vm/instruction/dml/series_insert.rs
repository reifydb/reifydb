// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{primitive::PrimitiveId, series::TimestampPrecision},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedPrimitive, ResolvedSeries},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::nodes::InsertSeriesNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};
use tracing::instrument;

use crate::vm::{
	services::Services,
	stack::SymbolTable,
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};

#[instrument(name = "mutate::series::insert", level = "trace", skip_all)]
pub(crate) fn insert_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: InsertSeriesNode,
	params: Params,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let series_name = plan.target.name();
	let Some(series_def) = services.catalog.find_series_by_name(txn, namespace.id, series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	// Get current metadata
	let Some(mut metadata) = services.catalog.find_series_metadata(txn, series_def.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();

	// Create resolved source for the series
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let series_ident = Fragment::internal(series_def.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series_def.clone());
	let resolved_source = Some(ResolvedPrimitive::Series(resolved_series));

	let execution_context = Arc::new(QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		stack: SymbolTable::new(),
	});

	let mut input_node = compile(*plan.input, txn, execution_context.clone());

	let mut inserted_count = 0u64;

	// Initialize the operator before execution
	input_node.initialize(txn, &execution_context)?;

	// Determine timestamp from clock based on precision
	let precision = series_def.precision;

	// Create schema for series encoding
	let schema = super::schema::get_or_create_series_schema(&services.catalog, &series_def, txn)?;

	// Process all input batches
	let mut mutable_context = (*execution_context).clone();
	while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
		let row_count = columns.row_count();

		for row_idx in 0..row_count {
			// Extract or generate timestamp
			let timestamp: i64 =
				if let Some(ts_col) = columns.iter().find(|col| col.name().text() == "timestamp") {
					match ts_col.data().get_value(row_idx) {
						Value::Int1(ts) => ts as i64,
						Value::Int2(ts) => ts as i64,
						Value::Int4(ts) => ts as i64,
						Value::Int8(ts) => ts,
						Value::Int16(ts) => ts as i64,
						Value::Uint1(ts) => ts as i64,
						Value::Uint2(ts) => ts as i64,
						Value::Uint4(ts) => ts as i64,
						Value::Uint8(ts) => ts as i64,
						Value::Uint16(ts) => ts as i64,
						_ => generate_timestamp(services, precision),
					}
				} else {
					generate_timestamp(services, precision)
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
			let key = SeriesRowKey {
				series: series_def.id,
				variant_tag,
				timestamp,
				sequence,
			};
			let encoded_key = key.encode();

			// Collect data column values (excluding timestamp and tag)
			let mut data_values = Vec::with_capacity(series_def.columns.len());
			for col_def in &series_def.columns {
				let value = if let Some(input_col) =
					columns.iter().find(|c| c.name().text() == col_def.name)
				{
					input_col.data().get_value(row_idx)
				} else {
					Value::none()
				};
				data_values.push(value);
			}

			// Encode using schema (timestamp at index 0, data columns at index 1+)
			let mut row = schema.allocate();
			schema.set_value(&mut row, 0, &Value::Int8(timestamp));
			for (i, value) in data_values.iter().enumerate() {
				schema.set_value(&mut row, i + 1, value);
			}

			// Write to storage
			txn.set(&encoded_key, row)?;

			// Track flow change for transactional/deferred view processing
			{
				let row_number = RowNumber::from(sequence as u64);
				let mut cols = Vec::with_capacity(1 + series_def.columns.len());
				cols.push(Column {
					name: Fragment::internal("timestamp"),
					data: ColumnData::int8(vec![timestamp]),
				});
				for (i, col_def) in series_def.columns.iter().enumerate() {
					let mut data = ColumnData::with_capacity(col_def.constraint.get_type(), 1);
					data.push_value(data_values[i].clone());
					cols.push(Column {
						name: Fragment::internal(&col_def.name),
						data,
					});
				}
				let post = Columns {
					row_numbers: CowVec::new(vec![row_number]),
					columns: CowVec::new(cols),
				};
				txn.track_flow_change(Change {
					origin: ChangeOrigin::Primitive(PrimitiveId::series(series_def.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Insert {
						post,
					}],
				});
			}

			// Update metadata
			if metadata.row_count == 0 {
				metadata.oldest_timestamp = timestamp;
			}
			if timestamp < metadata.oldest_timestamp {
				metadata.oldest_timestamp = timestamp;
			}
			if timestamp > metadata.newest_timestamp {
				metadata.newest_timestamp = timestamp;
			}
			metadata.row_count += 1;
			inserted_count += 1;
		}
	}

	// Save updated metadata
	services.catalog.update_series_metadata_txn(txn, metadata)?;

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("series", Value::Utf8(series_def.name)),
		("inserted", Value::Uint8(inserted_count)),
	]))
}

fn generate_timestamp(services: &Services, precision: TimestampPrecision) -> i64 {
	match precision {
		TimestampPrecision::Millisecond => services.clock.now_millis() as i64,
		TimestampPrecision::Microsecond => services.clock.now_micros() as i64,
		TimestampPrecision::Nanosecond => services.clock.now_nanos() as i64,
	}
}
