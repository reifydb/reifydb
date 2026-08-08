// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::object::ObjectId,
	key::{EncodableKey, column::ColumnKey, columns::ColumnsKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{CatalogStore, Result, catalog::Catalog};

pub(super) struct ColumnApplier;

impl CatalogChangeApplier for ColumnApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		reload_parent_columns(catalog, txn, key)
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		reload_parent_columns(catalog, txn, key)
	}
}

fn reload_parent_columns(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
	let object_id = if let Some(ck) = ColumnKey::decode(key) {
		Some(ck.object)
	} else if let Some(_ck) = ColumnsKey::decode(key) {
		return Ok(());
	} else {
		None
	};

	let object_id = match object_id {
		Some(id) => id,
		None => return Ok(()),
	};

	let version = txn.version();
	let columns = CatalogStore::list_columns(txn, object_id)?;

	match object_id {
		ObjectId::Table(id) => {
			if let Some(mut table) = catalog.cache.find_table_at(id, version) {
				table.columns = columns;
				catalog.cache.set_table(id, version, Some(table));
			}
		}
		ObjectId::View(id) => {
			if let Some(mut view) = catalog.cache.find_view_at(id, version) {
				*view.columns_mut() = columns;
				catalog.cache.set_view(id, version, Some(view));
			}
		}
		ObjectId::RingBuffer(id) => {
			if let Some(mut rb) = catalog.cache.find_ringbuffer_at(id, version) {
				rb.columns = columns;
				catalog.cache.set_ringbuffer(id, version, Some(rb));
			}
		}
		ObjectId::Series(id) => {
			if let Some(mut s) = catalog.cache.find_series_at(id, version) {
				s.columns = columns;
				catalog.cache.set_series(id, version, Some(s));
			}
		}
		_ => {}
	}

	Ok(())
}
