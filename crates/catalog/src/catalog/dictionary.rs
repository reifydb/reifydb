// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{DictionaryDef, DictionaryId, NamespaceId};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction, change::TransactionalDictionaryChanges};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::dictionary::find", level = "trace", skip(self, txn))]
	pub async fn find_dictionary<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: DictionaryId,
	) -> crate::Result<Option<DictionaryDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(dict) = TransactionalDictionaryChanges::find_dictionary(cmd, id) {
					return Ok(Some(dict.clone()));
				}

				// 2. Check if deleted
				if TransactionalDictionaryChanges::is_dictionary_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(dict) = self.materialized.find_dictionary(id, cmd.version()) {
					return Ok(Some(dict));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary(cmd, id).await? {
					warn!(
						"Dictionary with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(dict) = self.materialized.find_dictionary(id, qry.version()) {
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary(qry, id).await? {
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
	pub async fn find_dictionary_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<DictionaryDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(dict) =
					TransactionalDictionaryChanges::find_dictionary_by_name(cmd, namespace, name)
				{
					return Ok(Some(dict.clone()));
				}

				// 2. Check if deleted
				if TransactionalDictionaryChanges::is_dictionary_deleted_by_name(cmd, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(dict) =
					self.materialized.find_dictionary_by_name(namespace, name, cmd.version())
				{
					return Ok(Some(dict));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary_by_name(cmd, namespace, name).await? {
					warn!(
						"Dictionary '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(dict));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(dict) =
					self.materialized.find_dictionary_by_name(namespace, name, qry.version())
				{
					return Ok(Some(dict));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(dict) = CatalogStore::find_dictionary_by_name(qry, namespace, name).await? {
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
}
