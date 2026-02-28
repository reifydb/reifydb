// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSumTypeChangeOperations,
	id::NamespaceId,
	sumtype::{SumTypeDef, SumTypeKind, VariantDef},
};
use reifydb_transaction::{
	change::TransactionalSumTypeChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog, store::sumtype::create::SumTypeToCreate as StoreSumTypeToCreate};

#[derive(Debug, Clone)]
pub struct SumTypeToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub variants: Vec<VariantDef>,
	pub kind: SumTypeKind,
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
				kind: to_create.kind,
			},
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::sumtype::find", level = "trace", skip(self, txn))]
	pub fn find_sumtype(&self, txn: &mut Transaction<'_>, id: SumTypeId) -> Result<Option<SumTypeDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(def) = self.materialized.find_sumtype_at(id, cmd.version()) {
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype(&mut Transaction::Command(&mut *cmd), id)?
				{
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

				if let Some(def) = CatalogStore::find_sumtype(&mut Transaction::Admin(&mut *admin), id)?
				{
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

				if let Some(def) = CatalogStore::find_sumtype(&mut Transaction::Query(&mut *qry), id)? {
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
	pub fn find_sumtype_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<SumTypeDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(def) =
					self.materialized.find_sumtype_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(def));
				}

				if let Some(def) = CatalogStore::find_sumtype_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
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

				if let Some(def) = CatalogStore::find_sumtype_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
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

				if let Some(def) = CatalogStore::find_sumtype_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
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
	pub fn get_sumtype(&self, txn: &mut Transaction<'_>, id: SumTypeId) -> Result<SumTypeDef> {
		CatalogStore::get_sumtype(txn, id)
	}

	#[instrument(name = "catalog::sumtype::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_sumtype(&self, txn: &mut AdminTransaction, to_create: SumTypeToCreate) -> Result<SumTypeDef> {
		let def = CatalogStore::create_sumtype(txn, to_create.into())?;
		txn.track_sumtype_def_created(def.clone())?;
		Ok(def)
	}

	#[instrument(name = "catalog::sumtype::drop", level = "debug", skip(self, txn))]
	pub fn drop_sumtype(&self, txn: &mut AdminTransaction, sumtype: SumTypeDef) -> Result<()> {
		CatalogStore::drop_sumtype(txn, sumtype.id)?;
		txn.track_sumtype_def_deleted(sumtype)?;
		Ok(())
	}

	#[instrument(name = "catalog::sumtype::list", level = "debug", skip(self, txn))]
	pub fn list_sumtypes(&self, txn: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<SumTypeDef>> {
		CatalogStore::list_sumtypes(txn, namespace)
	}
}
