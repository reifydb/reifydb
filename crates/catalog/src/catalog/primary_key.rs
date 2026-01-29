// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	id::{ColumnId, PrimaryKeyId},
	key::PrimaryKeyDef,
	primitive::PrimitiveId,
};
use reifydb_transaction::transaction::{AsTransaction, command::CommandTransaction};
use tracing::instrument;

use crate::{
	CatalogStore, catalog::Catalog, store::primary_key::create::PrimaryKeyToCreate as StorePrimaryKeyToCreate,
};

#[derive(Debug, Clone)]
pub struct PrimaryKeyToCreate {
	pub source: PrimitiveId,
	pub column_ids: Vec<ColumnId>,
}

impl From<PrimaryKeyToCreate> for StorePrimaryKeyToCreate {
	fn from(to_create: PrimaryKeyToCreate) -> Self {
		StorePrimaryKeyToCreate {
			primitive: to_create.source,
			column_ids: to_create.column_ids,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::primary_key::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_primary_key(
		&self,
		txn: &mut CommandTransaction,
		to_create: PrimaryKeyToCreate,
	) -> crate::Result<PrimaryKeyId> {
		CatalogStore::create_primary_key(txn, to_create.into())
	}

	#[instrument(name = "catalog::primary_key::find", level = "trace", skip(self, txn, source))]
	pub fn find_primary_key<T: AsTransaction>(
		&self,
		txn: &mut T,
		source: impl Into<PrimitiveId>,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		CatalogStore::find_primary_key(txn, source)
	}

	#[instrument(name = "catalog::primary_key::list", level = "debug", skip(self, txn))]
	pub fn list_primary_keys<T: AsTransaction>(&self, txn: &mut T) -> crate::Result<Vec<PrimaryKeyDef>> {
		Ok(CatalogStore::list_primary_keys(txn)?.into_iter().map(|info| info.def).collect())
	}

	#[instrument(name = "catalog::primary_key::list_columns", level = "debug", skip(self, txn))]
	pub fn list_primary_key_columns<T: AsTransaction>(&self, txn: &mut T) -> crate::Result<Vec<(u64, u64, usize)>> {
		CatalogStore::list_primary_key_columns(txn)
	}
}
