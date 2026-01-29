// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::primitive::{PrimitiveDef, PrimitiveId};
use reifydb_transaction::transaction::AsTransaction;
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::primitive::find", level = "trace", skip(self, txn))]
	pub fn find_primitive<T: AsTransaction>(
		&self,
		txn: &mut T,
		id: PrimitiveId,
	) -> crate::Result<Option<PrimitiveDef>> {
		CatalogStore::find_primitive(txn, id)
	}

	#[instrument(name = "catalog::primitive::get", level = "trace", skip(self, txn))]
	pub fn get_primitive<T: AsTransaction>(&self, txn: &mut T, id: PrimitiveId) -> crate::Result<PrimitiveDef> {
		CatalogStore::get_primitive(txn, id)
	}
}
