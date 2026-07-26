// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	id::{ColumnId, PrimaryKeyId},
	key::PrimaryKey,
	object::ObjectId,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result, catalog::Catalog,
	store::primary_key::create::PrimaryKeyToCreate as StorePrimaryKeyToCreate,
};

#[derive(Debug, Clone)]
pub struct PrimaryKeyToCreate {
	pub object: ObjectId,
	pub column_ids: Vec<ColumnId>,
}

impl From<PrimaryKeyToCreate> for StorePrimaryKeyToCreate {
	fn from(to_create: PrimaryKeyToCreate) -> Self {
		StorePrimaryKeyToCreate {
			object: to_create.object,
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

	#[instrument(name = "catalog::primary_key::find", level = "trace", skip(self, txn, object))]
	pub fn find_primary_key(
		&self,
		txn: &mut Transaction<'_>,
		object: impl Into<ObjectId>,
	) -> Result<Option<PrimaryKey>> {
		let object = object.into();
		let cacheable = !matches!(&*txn, Transaction::Admin(_) | Transaction::Test(_));
		if cacheable
			&& let Some(primary_key_id) = self.cache.find_primary_key_id_by_object(object)
			&& let Some(primary_key) = self.cache.find_primary_key_at(primary_key_id, txn.version())
		{
			return Ok(Some(primary_key));
		}
		if let Some(primary_key) = CatalogStore::find_primary_key(txn, object)? {
			if cacheable {
				warn!("primary key for object {:?} found in storage but not in CatalogCache", object);
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
