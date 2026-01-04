// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table scan operator for VM-controlled batch-at-a-time scanning.

use reifydb_catalog::Catalog;
use reifydb_core::{
	Batch, EncodedKey, LazyBatch, LazyColumnMeta,
	interface::{EncodableKey, MultiVersionValues, NamespaceId, RowKey, RowKeyRange, TableDef},
	value::encoded::EncodedValuesLayout,
};
use reifydb_engine::StandardTransaction;
use reifydb_type::{Fragment, Type};

use crate::error::{Result, VmError};

/// State for scanning a table batch-at-a-time.
/// Stored in VM and used across multiple fetch operations.
pub struct ScanState {
	pub table_def: TableDef,
	pub row_layout: EncodedValuesLayout,
	pub storage_types: Vec<Type>,
	pub last_key: Option<EncodedKey>,
	pub exhausted: bool,
}

/// Operator for scanning tables from catalog storage.
///
/// This is a stateless helper that initializes scan state and fetches batches.
/// The VM owns the ScanState and controls when to fetch more data.
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

	/// Initialize scan state by looking up the table in the catalog.
	///
	/// Called once by the VM when executing a Source instruction.
	/// The returned ScanState is stored in the VM's active_scans map.
	pub async fn initialize<'a>(&self, catalog: &Catalog, tx: &mut StandardTransaction<'a>) -> Result<ScanState> {
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

		// Build storage types and layout
		let storage_types: Vec<_> = table_def.columns.iter().map(|col| col.constraint.get_type()).collect();
		let row_layout = EncodedValuesLayout::new(&storage_types);

		Ok(ScanState {
			table_def,
			row_layout,
			storage_types,
			last_key: None,
			exhausted: false,
		})
	}

	/// Fetch the next batch from storage.
	///
	/// Called repeatedly by the VM to fetch batches one at a time.
	/// Returns None when the scan is exhausted.
	pub async fn next_batch<'a>(
		state: &mut ScanState,
		tx: &mut StandardTransaction<'a>,
		batch_size: u64,
	) -> Result<Option<Batch>> {
		use futures_util::StreamExt;

		if state.exhausted {
			return Ok(None);
		}

		let range = RowKeyRange::scan_range(state.table_def.id.into(), state.last_key.as_ref());

		// Collect items from stream
		let mut items = Vec::new();
		let mut stream = tx
			.range(range, batch_size as usize)
			.map_err(|e| VmError::Internal(format!("storage error: {}", e)))?;

		while let Some(entry) = stream.next().await {
			let item = entry.map_err(|e| VmError::Internal(format!("storage error: {}", e)))?;
			items.push(item);
			if items.len() >= batch_size as usize {
				break;
			}
		}

		if items.is_empty() {
			state.exhausted = true;
			return Ok(None);
		}

		// Build LazyBatch from storage data
		let lazy_batch = build_lazy_batch(
			items,
			&state.table_def,
			&state.row_layout,
			&state.storage_types,
			&mut state.last_key,
		)?;

		Ok(Some(Batch::lazy(lazy_batch)))
	}
}

/// Build a LazyBatch from storage data.
///
/// This keeps data in encoded form for lazy evaluation through filters.
fn build_lazy_batch(
	batch: impl IntoIterator<Item = MultiVersionValues>,
	table_def: &TableDef,
	row_layout: &EncodedValuesLayout,
	storage_types: &[Type],
	last_key: &mut Option<EncodedKey>,
) -> Result<LazyBatch> {
	let mut rows = Vec::new();
	let mut row_numbers = Vec::new();

	for item in batch {
		if let Some(key) = RowKey::decode(&item.key) {
			rows.push(item.values);
			row_numbers.push(key.row);
			*last_key = Some(item.key);
		}
	}

	let column_metas = table_def
		.columns
		.iter()
		.enumerate()
		.map(|(idx, col)| LazyColumnMeta {
			name: Fragment::internal(&col.name),
			storage_type: storage_types[idx],
			output_type: col.constraint.get_type(),
			dictionary: None, // TODO: Add dictionary support
		})
		.collect();

	Ok(LazyBatch::new(rows, row_numbers, row_layout.clone(), column_metas))
}
