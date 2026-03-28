// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::schema::{Schema, SchemaId};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::schema::find", level = "trace", skip(self, txn))]
	pub fn find_schema(&self, txn: &mut Transaction<'_>, id: SchemaId) -> Result<Option<Schema>> {
		CatalogStore::find_schema(txn, id)
	}

	#[instrument(name = "catalog::schema::get", level = "trace", skip(self, txn))]
	pub fn get_schema(&self, txn: &mut Transaction<'_>, id: SchemaId) -> Result<Schema> {
		CatalogStore::get_schema(txn, id)
	}
}
