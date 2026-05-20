// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::shape::{Shape, ShapeId};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::shape::find", level = "trace", skip(self, txn))]
	pub fn find_shape(&self, txn: &mut Transaction<'_>, id: ShapeId) -> Result<Option<Shape>> {
		CatalogStore::find_shape(txn, id)
	}

	#[instrument(name = "catalog::shape::get", level = "trace", skip(self, txn))]
	pub fn get_shape(&self, txn: &mut Transaction<'_>, id: ShapeId) -> Result<Shape> {
		CatalogStore::get_shape(txn, id)
	}
}
