// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::catalog::series_not_found,
	interface::resolved::{ResolvedNamespace, ResolvedPrimitive, ResolvedSeries},
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DeleteSeriesNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, return_error, value::Value};
use tracing::instrument;

use crate::vm::{
	services::Services,
	stack::SymbolTable,
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};

#[instrument(name = "mutate::series::delete", level = "trace", skip_all)]
pub(crate) fn delete_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteSeriesNode,
	params: Params,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let namespace = services.catalog.find_namespace_by_name(txn, namespace_name)?.unwrap();

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

	// Create resolved source for the series
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let series_ident = Fragment::internal(series_def.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series_def.clone());
	let resolved_source = Some(ResolvedPrimitive::Series(resolved_series));

	let mut deleted_count = 0u64;

	if let Some(input_plan) = plan.input {
		// Delete rows matching the filter - collect keys from the scan results
		// The input plan is a pipeline (FROM series | FILTER ...) which goes through the
		// volcano series scan, so we get back rows with timestamp column.
		// We need to reconstruct SeriesRowKeys from the scanned data.
		let mut keys_to_delete = Vec::new();

		{
			let execution_context = Arc::new(QueryContext {
				services: services.clone(),
				source: resolved_source.clone(),
				batch_size: 1024,
				params: params.clone(),
				stack: SymbolTable::new(),
			});

			let mut input_node = compile(*input_plan, txn, execution_context.clone());
			input_node.initialize(txn, &execution_context)?;

			let mut mutable_context = (*execution_context).clone();
			while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
				let row_count = columns.row_count();
				let has_tag = series_def.tag.is_some();

				// Extract timestamps from the scan results
				let ts_col = columns.iter().find(|c| c.name().text() == "timestamp");
				let tag_col = if has_tag {
					columns.iter().find(|c| c.name().text() == "tag")
				} else {
					None
				};

				for row_idx in 0..row_count {
					let timestamp = ts_col
						.map(|c| match c.data().get_value(row_idx) {
							Value::Int8(ts) => ts,
							_ => 0,
						})
						.unwrap_or(0);

					let variant_tag = if has_tag {
						tag_col.map(|c| match c.data().get_value(row_idx) {
							Value::Uint1(t) => Some(t),
							_ => Some(0),
						})
						.unwrap_or(Some(0))
					} else {
						None
					};

					// We need the sequence too, but the scan doesn't expose it directly.
					// Instead, scan the storage for matching timestamp+tag to find actual keys.
					let range = SeriesRowKeyRange::scan_range(
						series_def.id,
						variant_tag,
						Some(timestamp),
						Some(timestamp),
						None,
					);
					let mut stream = txn.range(range, 1024)?;
					while let Some(entry) = stream.next() {
						let entry = entry?;
						if let Some(key) = SeriesRowKey::decode(&entry.key) {
							if key.timestamp == timestamp && key.variant_tag == variant_tag
							{
								keys_to_delete.push(entry.key);
							}
						}
					}
				}
			}
		}

		// Deduplicate keys
		keys_to_delete.sort();
		keys_to_delete.dedup();

		// Delete the collected keys
		for key in &keys_to_delete {
			txn.remove(key)?;
			deleted_count += 1;
		}
	} else {
		// Delete all rows - scan the full range and delete
		let range = SeriesRowKeyRange::full_scan(series_def.id, None);
		let mut keys_to_delete = Vec::new();

		let mut stream = txn.range(range, 4096)?;
		while let Some(entry) = stream.next() {
			let entry = entry?;
			keys_to_delete.push(entry.key);
		}
		drop(stream);

		for key in &keys_to_delete {
			txn.remove(key)?;
			deleted_count += 1;
		}
	}

	// Update metadata
	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_timestamp = 0;
		metadata.newest_timestamp = 0;
	}

	services.catalog.update_series_metadata_txn(txn, metadata)?;

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("series", Value::Utf8(series_def.name)),
		("deleted", Value::Uint8(deleted_count)),
	]))
}
