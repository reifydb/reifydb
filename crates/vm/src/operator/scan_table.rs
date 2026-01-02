// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table scan operator for creating pipelines from catalog tables.

use reifydb_catalog::Catalog;
use reifydb_core::{
	interface::{EncodableKey, NamespaceId, RowKey, RowKeyRange, TableDef},
	value::{
		column::{Column, ColumnData, Columns},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_engine::StandardTransaction;
use reifydb_type::Fragment;

use crate::{
	error::{Result, VmError},
	pipeline::Pipeline,
};

/// Operator for scanning tables from catalog storage.
///
/// This is a source operator that creates a new pipeline from a table name
/// by looking it up in the catalog and scanning its data from storage.
pub struct ScanTableOp {
	pub table_name: String,
	pub batch_size: u64,
}

impl ScanTableOp {
	/// Create a new table scan operator.
	pub fn new(table_name: String, batch_size: u64) -> Self {
		Self {
			table_name,
			batch_size,
		}
	}

	/// Create a pipeline by scanning a table from the catalog.
	///
	/// This performs namespace resolution (qualified vs simple names),
	/// looks up the table definition, and scans all data from storage.
	pub async fn create<'a>(&self, catalog: &Catalog, tx: &mut StandardTransaction<'a>) -> Result<Pipeline> {
		// Resolve namespace and table name
		let (namespace_id, table_name) = if let Some((ns, tbl)) = self.table_name.split_once('.') {
			// Qualified name: look up namespace by name
			let namespace_def = catalog
				.find_namespace_by_name(tx, ns)
				.await
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?
				.ok_or_else(|| VmError::NamespaceNotFound {
					name: ns.to_string(),
				})?;
			(namespace_def.id, tbl)
		} else {
			// Simple name: use default namespace ID = 1
			(NamespaceId(1), self.table_name.as_str())
		};

		// Look up table from catalog
		let table_def = catalog
			.find_table_by_name(tx, namespace_id, table_name)
			.await
			.map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})?
			.ok_or_else(|| VmError::TableNotFound {
				name: self.table_name.clone(),
			})?;

		// Scan the table
		let columns = scan_table(&table_def, tx, self.batch_size).await?;

		// Create pipeline from scanned data
		Ok(create_pipeline_from_columns(columns))
	}
}

/// Scan a table and return all rows as a single Columns batch.
///
/// This materializes the entire table immediately. For large tables,
/// a streaming approach would be more appropriate.
async fn scan_table<'a>(table_def: &TableDef, rx: &mut StandardTransaction<'a>, batch_size: u64) -> Result<Columns> {
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
			.map_err(|e| VmError::Internal(format!("storage error: {}", e)))?;

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
			.map_err(|e| VmError::Internal(format!("column append error: {}", e)))?;
	}

	Ok(result)
}

/// Create a pipeline that yields a single batch from pre-scanned data.
fn create_pipeline_from_columns(columns: Columns) -> Pipeline {
	Box::pin(futures_util::stream::once(async move { Ok(columns) }))
}
