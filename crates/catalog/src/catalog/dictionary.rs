// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackDictionaryChangeOperations, dictionary::DictionaryDef, id::NamespaceId,
};
use reifydb_transaction::{
	change::TransactionalDictionaryChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	value::{dictionary::DictionaryId, r#type::Type},
};
use tracing::{instrument, warn};

use crate::{CatalogStore, catalog::Catalog, store::dictionary::create::DictionaryToCreate as StoreDictionaryToCreate};

#[derive(Debug, Clone)]
pub struct DictionaryToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub value_type: Type,
	pub id_type: Type,
}

impl From<DictionaryToCreate> for StoreDictionaryToCreate {
	fn from(to_create: DictionaryToCreate) -> Self {
		StoreDictionaryToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			value_type: to_create.value_type,
			id_type: to_create.id_type,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::dictionary::find", level = "trace", skip(self, txn))]
	pub fn find_dictionary(
		&self,
		txn: &mut Transaction<'_>,
		id: DictionaryId,
	) -> crate::Result<Option<DictionaryDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(dict) = self.materialized.find_dictionary_at(id, cmd.version()) {
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) =
					CatalogStore::find_dictionary(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"Dictionary with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(dict) = TransactionalDictionaryChanges::find_dictionary(admin, id) {
					return Ok(Some(dict.clone()));
				}

				// 2. Check if deleted
				if TransactionalDictionaryChanges::is_dictionary_deleted(admin, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(dict) = self.materialized.find_dictionary_at(id, admin.version()) {
					return Ok(Some(dict));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(dict) =
					CatalogStore::find_dictionary(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"Dictionary with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(dict) = self.materialized.find_dictionary_at(id, qry.version()) {
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) =
					CatalogStore::find_dictionary(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!(
						"Dictionary with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::dictionary::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_dictionary_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<DictionaryDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(dict) =
					self.materialized.find_dictionary_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(dict) =
					TransactionalDictionaryChanges::find_dictionary_by_name(admin, namespace, name)
				{
					return Ok(Some(dict.clone()));
				}

				// 2. Check if deleted
				if TransactionalDictionaryChanges::is_dictionary_deleted_by_name(admin, namespace, name)
				{
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(dict) =
					self.materialized.find_dictionary_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(dict));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(dict) =
					self.materialized.find_dictionary_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::dictionary::get", level = "trace", skip(self, txn))]
	pub fn get_dictionary(&self, txn: &mut Transaction<'_>, id: DictionaryId) -> crate::Result<DictionaryDef> {
		CatalogStore::get_dictionary(txn, id)
	}

	#[instrument(name = "catalog::dictionary::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_dictionary(
		&self,
		txn: &mut AdminTransaction,
		to_create: DictionaryToCreate,
	) -> crate::Result<DictionaryDef> {
		let dictionary = CatalogStore::create_dictionary(txn, to_create.into())?;
		txn.track_dictionary_def_created(dictionary.clone())?;
		Ok(dictionary)
	}

	#[instrument(name = "catalog::dictionary::list", level = "debug", skip(self, txn))]
	pub fn list_dictionaries(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
	) -> crate::Result<Vec<DictionaryDef>> {
		CatalogStore::list_dictionaries(txn, namespace)
	}

	#[instrument(name = "catalog::dictionary::list_all", level = "debug", skip(self, txn))]
	pub fn list_all_dictionaries(&self, txn: &mut Transaction<'_>) -> crate::Result<Vec<DictionaryDef>> {
		CatalogStore::list_all_dictionaries(txn)
	}
}
