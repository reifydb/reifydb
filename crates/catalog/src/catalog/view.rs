// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackViewChangeOperations,
		id::{NamespaceId, PrimaryKeyId, ViewId},
		view::ViewDef,
	},
};
use reifydb_transaction::{
	change::TransactionalViewChanges,
	standard::{IntoStandardTransaction, StandardTransaction, command::StandardCommandTransaction},
};
use reifydb_type::{error, fragment::Fragment, internal, value::constraint::TypeConstraint};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	store::view::create::{ViewColumnToCreate as StoreViewColumnToCreate, ViewToCreate as StoreViewToCreate},
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub fragment: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub fragment: Option<Fragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub columns: Vec<ViewColumnToCreate>,
}

impl From<ViewColumnToCreate> for StoreViewColumnToCreate {
	fn from(col: ViewColumnToCreate) -> Self {
		StoreViewColumnToCreate {
			name: col.name,
			constraint: col.constraint,
			fragment: col.fragment,
		}
	}
}

impl From<ViewToCreate> for StoreViewToCreate {
	fn from(to_create: ViewToCreate) -> Self {
		StoreViewToCreate {
			fragment: to_create.fragment,
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::view::find", level = "trace", skip(self, txn))]
	pub fn find_view<T: IntoStandardTransaction>(&self, txn: &mut T, id: ViewId) -> crate::Result<Option<ViewDef>> {
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
				if let Some(view) = CatalogStore::find_view(cmd, id)? {
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
				if let Some(view) = CatalogStore::find_view(qry, id)? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::view::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_view_by_name<T: IntoStandardTransaction>(
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
				if let Some(view) = CatalogStore::find_view_by_name(cmd, namespace, name)? {
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
				if let Some(view) = CatalogStore::find_view_by_name(qry, namespace, name)? {
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
	pub fn get_view<T: IntoStandardTransaction>(&self, txn: &mut T, id: ViewId) -> crate::Result<ViewDef> {
		self.find_view(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::view::create_deferred", level = "debug", skip(self, txn, to_create))]
	pub fn create_deferred_view(
		&self,
		txn: &mut StandardCommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		let view = CatalogStore::create_deferred_view(txn, to_create.into())?;
		txn.track_view_def_created(view.clone())?;

		let schema = Schema::from(view.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::create_transactional", level = "debug", skip(self, txn, to_create))]
	pub fn create_transactional_view(
		&self,
		txn: &mut StandardCommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		let view = CatalogStore::create_transactional_view(txn, to_create.into())?;
		txn.track_view_def_created(view.clone())?;

		let schema = Schema::from(view.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::list_all", level = "debug", skip(self, txn))]
	pub fn list_views_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<ViewDef>> {
		CatalogStore::list_views_all(txn)
	}

	#[instrument(name = "catalog::view::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_view_primary_key(
		&self,
		txn: &mut StandardCommandTransaction,
		view_id: ViewId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		CatalogStore::set_view_primary_key(txn, view_id, primary_key_id)
	}

	#[instrument(name = "catalog::view::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_view_pk_id<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		view_id: ViewId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		CatalogStore::get_view_pk_id(txn, view_id)
	}
}
