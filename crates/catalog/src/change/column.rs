// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::schema::SchemaId,
	key::{EncodableKey, column::ColumnKey, columns::ColumnsKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{CatalogStore, Result, catalog::Catalog};

pub(super) struct ColumnApplier;

impl CatalogChangeApplier for ColumnApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		reload_parent_columns(catalog, txn, key)
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		reload_parent_columns(catalog, txn, key)
	}
}

fn reload_parent_columns(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
	let schema_id = if let Some(ck) = ColumnKey::decode(key) {
		Some(ck.object)
	} else if let Some(_ck) = ColumnsKey::decode(key) {
		// ColumnsKey only has column_id, no parent schema — cannot determine parent
		return Ok(());
	} else {
		None
	};

	let schema_id = match schema_id {
		Some(id) => id,
		None => return Ok(()),
	};

	let version = txn.version();
	let columns = CatalogStore::list_columns(txn, schema_id)?;

	match schema_id {
		SchemaId::Table(id) => {
			if let Some(mut table) = catalog.materialized.find_table_at(id, version) {
				table.columns = columns;
				catalog.materialized.set_table(id, version, Some(table));
			}
		}
		SchemaId::View(id) => {
			if let Some(mut view) = catalog.materialized.find_view_at(id, version) {
				*view.columns_mut() = columns;
				catalog.materialized.set_view(id, version, Some(view));
			}
		}
		SchemaId::RingBuffer(id) => {
			if let Some(mut rb) = catalog.materialized.find_ringbuffer_at(id, version) {
				rb.columns = columns;
				catalog.materialized.set_ringbuffer(id, version, Some(rb));
			}
		}
		_ => {}
	}

	Ok(())
}
