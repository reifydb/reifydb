// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{ringbuffer::EvictionVisibility, storage::StorageId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	pub fn create_ttl_eviction(
		&self,
		txn: &mut AdminTransaction,
		storage: StorageId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		CatalogStore::store_ttl_eviction(txn, storage, eviction)
	}

	pub fn find_ttl_eviction(&self, txn: &mut Transaction<'_>, storage: StorageId) -> Result<EvictionVisibility> {
		CatalogStore::find_ttl_eviction(txn, storage)
	}
}
