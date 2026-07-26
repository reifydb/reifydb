// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::object::{Object, ObjectId};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::object::find", level = "trace", skip(self, txn))]
	pub fn find_object(&self, txn: &mut Transaction<'_>, id: ObjectId) -> Result<Option<Object>> {
		CatalogStore::find_object(txn, id)
	}

	#[instrument(name = "catalog::object::get", level = "trace", skip(self, txn))]
	pub fn get_object(&self, txn: &mut Transaction<'_>, id: ObjectId) -> Result<Object> {
		CatalogStore::get_object(txn, id)
	}
}
