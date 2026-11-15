// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{ringbuffer::EvictionVisibility, storage::StorageId};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::error::ValueError;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	pub fn create_ttl_eviction(
		&self,
		txn: &mut AdminTransaction,
		storage: StorageId,
		eviction: EvictionVisibility,
	) -> Result<(), ValueError> {
		CatalogStore::store_ttl_eviction(txn, storage, eviction)
	}

	pub fn find_ttl_eviction(
		&self,
		txn: &mut Transaction<'_>,
		storage: StorageId,
	) -> Result<EvictionVisibility, ValueError> {
		CatalogStore::find_ttl_eviction(txn, storage)
	}
}
