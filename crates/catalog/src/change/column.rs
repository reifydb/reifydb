// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::shape::ShapeId,
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
	let shape_id = if let Some(ck) = ColumnKey::decode(key) {
		Some(ck.shape)
	} else if let Some(_ck) = ColumnsKey::decode(key) {
		return Ok(());
	} else {
		None
	};

	let shape_id = match shape_id {
		Some(id) => id,
		None => return Ok(()),
	};

	let version = txn.version();
	let columns = CatalogStore::list_columns(txn, shape_id)?;

	match shape_id {
		ShapeId::Table(id) => {
			if let Some(mut table) = catalog.cache.find_table_at(id, version) {
				table.columns = columns;
				catalog.cache.set_table(id, version, Some(table));
			}
		}
		ShapeId::View(id) => {
			if let Some(mut view) = catalog.cache.find_view_at(id, version) {
				*view.columns_mut() = columns;
				catalog.cache.set_view(id, version, Some(view));
			}
		}
		ShapeId::RingBuffer(id) => {
			if let Some(mut rb) = catalog.cache.find_ringbuffer_at(id, version) {
				rb.columns = columns;
				catalog.cache.set_ringbuffer(id, version, Some(rb));
			}
		}
		ShapeId::Series(id) => {
			if let Some(mut s) = catalog.cache.find_series_at(id, version) {
				s.columns = columns;
				catalog.cache.set_series(id, version, Some(s));
			}
		}
		_ => {}
	}

	Ok(())
}
