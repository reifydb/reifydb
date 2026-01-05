// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceId, ViewDef, ViewId};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction, change::TransactionalViewChanges};
use reifydb_type::{error, internal};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::view::find", level = "trace", skip(self, txn))]
	pub async fn find_view<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: ViewId,
	) -> crate::Result<Option<ViewDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(view) = TransactionalViewChanges::find_view(cmd, id) {
					return Ok(Some(view.clone()));
				}

				// 2. Check if deleted
				if TransactionalViewChanges::is_view_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(view) = self.materialized.find_view_at(id, cmd.version()) {
					return Ok(Some(view));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view(cmd, id).await? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(view) = self.materialized.find_view_at(id, qry.version()) {
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view(qry, id).await? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::view::find_by_name", level = "trace", skip(self, txn, name))]
	pub async fn find_view_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<ViewDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(view) = TransactionalViewChanges::find_view_by_name(cmd, namespace, name) {
					return Ok(Some(view.clone()));
				}

				// 2. Check if deleted
				if TransactionalViewChanges::is_view_deleted_by_name(cmd, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(view) =
					self.materialized.find_view_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(view));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view_by_name(cmd, namespace, name).await? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(view) =
					self.materialized.find_view_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view_by_name(qry, namespace, name).await? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::view::get", level = "trace", skip(self, txn))]
	pub async fn get_view<T: IntoStandardTransaction>(&self, txn: &mut T, id: ViewId) -> crate::Result<ViewDef> {
		self.find_view(txn, id).await?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}
}
