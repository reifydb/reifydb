// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table scan functionality for creating pipelines from storage.

use reifydb_core::{
	interface::{EncodableKey, RowKey, RowKeyRange, TableDef},
	value::{
		column::{Column, ColumnData, Columns},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_engine::StandardTransaction;
use reifydb_type::Fragment;

use crate::{error::Result, pipeline::Pipeline};

/// Scan a table and return all rows as a single Columns batch.
///
/// This materializes the entire table immediately. For large tables,
/// a streaming approach would be more appropriate.
pub async fn scan_table<'a>(
	table_def: &TableDef,
	rx: &mut StandardTransaction<'a>,
	batch_size: u64,
) -> Result<Columns> {
	// Build storage types from column definitions
	let storage_types: Vec<_> = table_def.columns.iter().map(|col| col.constraint.get_type()).collect();

	let row_layout = EncodedValuesLayout::new(&storage_types);

	// Create empty columns with proper types
	let empty_columns: Vec<Column> = table_def
		.columns
		.iter()
		.enumerate()
		.map(|(idx, col)| Column {
			name: Fragment::internal(&col.name),
			data: ColumnData::with_capacity(storage_types[idx], 0),
		})
		.collect();

	let mut result = Columns::with_row_numbers(empty_columns, Vec::new());
	let mut last_key: Option<reifydb_core::EncodedKey> = None;

	loop {
		let range = RowKeyRange::scan_range(table_def.id.into(), last_key.as_ref());
		let batch = rx
			.range_batch(range, batch_size)
			.await
			.map_err(|e| crate::error::VmError::Internal(format!("storage error: {}", e)))?;

		if batch.items.is_empty() {
			break;
		}

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();

		for multi in batch.items.into_iter().take(batch_size as usize) {
			if let Some(key) = RowKey::decode(&multi.key) {
				batch_rows.push(multi.values);
				row_numbers.push(key.row);
				last_key = Some(multi.key);
			}
		}

		if batch_rows.is_empty() {
			break;
		}

		result.append_rows(&row_layout, batch_rows.into_iter(), row_numbers)
			.map_err(|e| crate::error::VmError::Internal(format!("column append error: {}", e)))?;
	}

	Ok(result)
}

/// Create a pipeline that yields a single batch from a pre-scanned table.
pub fn create_pipeline_from_columns(columns: Columns) -> Pipeline {
	Box::pin(futures_util::stream::once(async move { Ok(columns) }))
}

/// Scan a table and create a pipeline that yields the results.
///
/// This materializes the entire table first, then wraps it in a stream.
pub async fn create_table_scan_pipeline<'a>(
	table_def: &TableDef,
	rx: &mut StandardTransaction<'a>,
	batch_size: u64,
) -> Result<Pipeline> {
	let columns = scan_table(table_def, rx, batch_size).await?;
	Ok(create_pipeline_from_columns(columns))
}
