// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSumTypeChangeOperations,
	id::NamespaceId,
	sumtype::{SumTypeDef, VariantDef},
};
use reifydb_transaction::{
	change::TransactionalSumTypeChanges,
	transaction::{AsTransaction, Transaction, admin::AdminTransaction},
};
use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};
use tracing::{instrument, warn};

use crate::{CatalogStore, catalog::Catalog, store::sumtype::create::SumTypeToCreate as StoreSumTypeToCreate};

#[derive(Debug, Clone)]
pub struct SumTypeToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub variants: Vec<VariantDef>,
}

impl From<SumTypeToCreate> for StoreSumTypeToCreate {
	fn from(to_create: SumTypeToCreate) -> Self {
		StoreSumTypeToCreate {
			name: to_create.name.clone(),
			namespace: to_create.namespace,
			def: SumTypeDef {
				id: SumTypeId(0),
				namespace: to_create.namespace,
				name: to_create.name.text().to_string(),
				variants: to_create.variants,
			},
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::sumtype::find", level = "trace", skip(self, txn))]
	pub fn find_sumtype<T: AsTransaction>(&self, txn: &mut T, id: SumTypeId) -> crate::Result<Option<SumTypeDef>> {
		match txn.as_transaction() {
			Transaction::Command(cmd) => {
				if let Some(def) = self.materialized.find_sumtype_at(id, cmd.version()) {
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype(cmd, id)? {
					warn!(
						"SumType with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(def) = TransactionalSumTypeChanges::find_sumtype(admin, id) {
					return Ok(Some(def.clone()));
				}

				if TransactionalSumTypeChanges::is_sumtype_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(def) = self.materialized.find_sumtype_at(id, admin.version()) {
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype(admin, id)? {
					warn!(
						"SumType with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(def) = self.materialized.find_sumtype_at(id, qry.version()) {
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype(qry, id)? {
					warn!(
						"SumType with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::sumtype::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_sumtype_by_name<T: AsTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<SumTypeDef>> {
		match txn.as_transaction() {
			Transaction::Command(cmd) => {
				if let Some(def) =
					self.materialized.find_sumtype_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype_by_name(cmd, namespace, name)? {
					warn!(
						"SumType '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(def) =
					TransactionalSumTypeChanges::find_sumtype_by_name(admin, namespace, name)
				{
					return Ok(Some(def.clone()));
				}

				if TransactionalSumTypeChanges::is_sumtype_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(def) =
					self.materialized.find_sumtype_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype_by_name(admin, namespace, name)? {
					warn!(
						"SumType '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(def) =
					self.materialized.find_sumtype_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype_by_name(qry, namespace, name)? {
					warn!(
						"SumType '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(def));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::sumtype::get", level = "trace", skip(self, txn))]
	pub fn get_sumtype<T: AsTransaction>(&self, txn: &mut T, id: SumTypeId) -> crate::Result<SumTypeDef> {
		CatalogStore::get_sumtype(txn, id)
	}

	#[instrument(name = "catalog::sumtype::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_sumtype(
		&self,
		txn: &mut AdminTransaction,
		to_create: SumTypeToCreate,
	) -> crate::Result<SumTypeDef> {
		let def = CatalogStore::create_sumtype(txn, to_create.into())?;
		txn.track_sumtype_def_created(def.clone())?;
		Ok(def)
	}

	#[instrument(name = "catalog::sumtype::list", level = "debug", skip(self, txn))]
	pub fn list_sumtypes<T: AsTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
	) -> crate::Result<Vec<SumTypeDef>> {
		CatalogStore::list_sumtypes(txn, namespace)
	}
}
