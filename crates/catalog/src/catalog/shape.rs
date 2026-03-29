// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
