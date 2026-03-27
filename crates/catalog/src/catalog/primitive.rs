// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::primitive::{Primitive, PrimitiveId};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::primitive::find", level = "trace", skip(self, txn))]
	pub fn find_primitive(&self, txn: &mut Transaction<'_>, id: PrimitiveId) -> Result<Option<Primitive>> {
		CatalogStore::find_primitive(txn, id)
	}

	#[instrument(name = "catalog::primitive::get", level = "trace", skip(self, txn))]
	pub fn get_primitive(&self, txn: &mut Transaction<'_>, id: PrimitiveId) -> Result<Primitive> {
		CatalogStore::get_primitive(txn, id)
	}
}
