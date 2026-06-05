// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	id::{ColumnId, PrimaryKeyId},
	key::PrimaryKey,
	shape::ShapeId,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result, catalog::Catalog,
	store::primary_key::create::PrimaryKeyToCreate as StorePrimaryKeyToCreate,
};

#[derive(Debug, Clone)]
pub struct PrimaryKeyToCreate {
	pub shape: ShapeId,
	pub column_ids: Vec<ColumnId>,
}

impl From<PrimaryKeyToCreate> for StorePrimaryKeyToCreate {
	fn from(to_create: PrimaryKeyToCreate) -> Self {
		StorePrimaryKeyToCreate {
			shape: to_create.shape,
			column_ids: to_create.column_ids,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::primary_key::create", level = "info", skip(self, txn, to_create))]
	pub fn create_primary_key(
		&self,
		txn: &mut AdminTransaction,
		to_create: PrimaryKeyToCreate,
	) -> Result<PrimaryKeyId> {
		CatalogStore::create_primary_key(txn, to_create.into())
	}

	#[instrument(name = "catalog::primary_key::find", level = "trace", skip(self, txn, shape))]
	pub fn find_primary_key(
		&self,
		txn: &mut Transaction<'_>,
		shape: impl Into<ShapeId>,
	) -> Result<Option<PrimaryKey>> {
		let shape = shape.into();
		let cacheable = !matches!(&*txn, Transaction::Admin(_) | Transaction::Test(_));
		if cacheable
			&& let Some(primary_key_id) = self.cache.find_primary_key_id_by_shape(shape)
			&& let Some(primary_key) = self.cache.find_primary_key_at(primary_key_id, txn.version())
		{
			return Ok(Some(primary_key));
		}
		if let Some(primary_key) = CatalogStore::find_primary_key(txn, shape)? {
			if cacheable {
				warn!("primary key for shape {:?} found in storage but not in CatalogCache", shape);
			}
			return Ok(Some(primary_key));
		}
		Ok(None)
	}

	#[instrument(name = "catalog::primary_key::list", level = "trace", skip(self, txn))]
	pub fn list_primary_keys(&self, txn: &mut Transaction<'_>) -> Result<Vec<PrimaryKey>> {
		Ok(CatalogStore::list_primary_keys(txn)?.into_iter().map(|info| info.def).collect())
	}

	#[instrument(name = "catalog::primary_key::list_columns", level = "trace", skip(self, txn))]
	pub fn list_primary_key_columns(&self, txn: &mut Transaction<'_>) -> Result<Vec<(u64, u64, usize)>> {
		CatalogStore::list_primary_key_columns(txn)
	}
}
