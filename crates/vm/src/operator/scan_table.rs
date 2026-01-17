// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table scan operator for VM-controlled batch-at-a-time scanning.

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::{key::EncodedKey, schema::Schema},
	interface::{
		catalog::{id::NamespaceId, table::TableDef},
		store::MultiVersionValues,
	},
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::batch::{
		Batch,
		lazy::{LazyBatch, LazyColumnMeta},
	},
};
use reifydb_transaction::standard::StandardTransaction;
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::error::{Result, VmError};

/// State for scanning a table batch-at-a-time.
/// Stored in VM and used across multiple fetch operations.
pub struct ScanState {
	pub table_def: TableDef,
	pub schema: Option<Schema>,
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
	pub fn initialize<'a>(&self, catalog: &Catalog, tx: &mut StandardTransaction<'a>) -> Result<ScanState> {
		// Resolve namespace and table name
		let (namespace_id, table_name) = if let Some((ns, tbl)) = self.table_name.split_once('.') {
			// Qualified name: look up namespace by name
			let namespace_def = catalog
				.find_namespace_by_name(tx, ns)
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
			.map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})?
			.ok_or_else(|| VmError::TableNotFound {
				name: self.table_name.clone(),
			})?;

		// Build storage types
		let storage_types: Vec<_> = table_def.columns.iter().map(|col| col.constraint.get_type()).collect();

		Ok(ScanState {
			table_def,
			schema: None,
			storage_types,
			last_key: None,
			exhausted: false,
		})
	}

	/// Fetch the next batch from storage.
	///
	/// Called repeatedly by the VM to fetch batches one at a time.
	/// Returns None when the scan is exhausted.
	pub fn next_batch<'a>(
		catalog: &Catalog,
		state: &mut ScanState,
		tx: &mut StandardTransaction<'a>,
		batch_size: u64,
	) -> Result<Option<Batch>> {
		if state.exhausted {
			return Ok(None);
		}

		let range = RowKeyRange::scan_range(state.table_def.id.into(), state.last_key.as_ref());

		// Collect items from stream
		let mut items = Vec::new();
		let mut stream = tx
			.range(range, batch_size as usize)
			.map_err(|e| VmError::Internal(format!("storage error: {}", e)))?;

		while let Some(entry) = stream.next() {
			let item = entry.map_err(|e| VmError::Internal(format!("storage error: {}", e)))?;
			items.push(item);
			if items.len() >= batch_size as usize {
				break;
			}
		}

		// Drop the stream to release the borrow on tx
		drop(stream);

		if items.is_empty() {
			state.exhausted = true;
			return Ok(None);
		}

		// Build LazyBatch from storage data
		let lazy_batch = build_lazy_batch(
			catalog,
			items,
			&state.table_def,
			&mut state.schema,
			&state.storage_types,
			&mut state.last_key,
			tx,
		)?;

		Ok(Some(Batch::lazy(lazy_batch)))
	}
}

/// Build a LazyBatch from storage data.
///
/// This keeps data in encoded form for lazy evaluation through filters.
fn build_lazy_batch(
	catalog: &Catalog,
	batch: impl IntoIterator<Item = MultiVersionValues>,
	table_def: &TableDef,
	schema_cache: &mut Option<Schema>,
	storage_types: &[Type],
	last_key: &mut Option<EncodedKey>,
	tx: &mut StandardTransaction,
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

	// Load schema from first row if not cached
	let schema = if let Some(schema) = schema_cache {
		schema.clone()
	} else if !rows.is_empty() {
		let fingerprint = rows[0].fingerprint();
		let schema = catalog
			.schema
			.get_or_load(fingerprint, tx)
			.map_err(|e: reifydb_type::error::Error| VmError::CatalogError {
				message: e.to_string(),
			})?
			.ok_or_else(|| VmError::CatalogError {
				message: format!(
					"Schema with fingerprint {:?} not found for table {}",
					fingerprint, table_def.name
				),
			})?;
		*schema_cache = Some(schema.clone());
		schema
	} else {
		return Err(VmError::CatalogError {
			message: "Cannot create LazyBatch without rows".to_string(),
		});
	};

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

	Ok(LazyBatch::new(rows, row_numbers, &schema, column_metas))
}
