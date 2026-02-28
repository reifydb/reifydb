// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackViewChangeOperations,
		id::{NamespaceId, PrimaryKeyId, ViewId},
		view::ViewDef,
	},
	internal,
};
use reifydb_transaction::{
	change::TransactionalViewChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{error, fragment::Fragment, value::constraint::TypeConstraint};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::view::create::{ViewColumnToCreate as StoreViewColumnToCreate, ViewToCreate as StoreViewToCreate},
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<ViewColumnToCreate>,
}

impl From<ViewColumnToCreate> for StoreViewColumnToCreate {
	fn from(col: ViewColumnToCreate) -> Self {
		StoreViewColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
		}
	}
}

impl From<ViewToCreate> for StoreViewToCreate {
	fn from(to_create: ViewToCreate) -> Self {
		StoreViewToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::view::find", level = "trace", skip(self, txn))]
	pub fn find_view(&self, txn: &mut Transaction<'_>, id: ViewId) -> Result<Option<ViewDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(view) = self.materialized.find_view_at(id, cmd.version()) {
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view(&mut Transaction::Command(&mut *cmd), id)? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(view) = TransactionalViewChanges::find_view(admin, id) {
					return Ok(Some(view.clone()));
				}

				// 2. Check if deleted
				if TransactionalViewChanges::is_view_deleted(admin, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(view) = self.materialized.find_view_at(id, admin.version()) {
					return Ok(Some(view));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view(&mut Transaction::Admin(&mut *admin), id)? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(view) = self.materialized.find_view_at(id, qry.version()) {
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view(&mut Transaction::Query(&mut *qry), id)? {
					warn!("View with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::view::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_view_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<ViewDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(view) =
					self.materialized.find_view_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(view) = TransactionalViewChanges::find_view_by_name(admin, namespace, name)
				{
					return Ok(Some(view.clone()));
				}

				// 2. Check if deleted
				if TransactionalViewChanges::is_view_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(view) =
					self.materialized.find_view_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(view));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(view) =
					self.materialized.find_view_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(view));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
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
	pub fn get_view(&self, txn: &mut Transaction<'_>, id: ViewId) -> Result<ViewDef> {
		self.find_view(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::view::create_deferred", level = "debug", skip(self, txn, to_create))]
	pub fn create_deferred_view(&self, txn: &mut AdminTransaction, to_create: ViewToCreate) -> Result<ViewDef> {
		let view = CatalogStore::create_deferred_view(txn, to_create.into())?;
		txn.track_view_def_created(view.clone())?;

		let schema = Schema::from(view.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::create_transactional", level = "debug", skip(self, txn, to_create))]
	pub fn create_transactional_view(
		&self,
		txn: &mut AdminTransaction,
		to_create: ViewToCreate,
	) -> Result<ViewDef> {
		let view = CatalogStore::create_transactional_view(txn, to_create.into())?;
		txn.track_view_def_created(view.clone())?;

		let schema = Schema::from(view.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::drop", level = "debug", skip(self, txn))]
	pub fn drop_view(&self, txn: &mut AdminTransaction, view: ViewDef) -> Result<()> {
		CatalogStore::drop_view(txn, view.id)?;
		txn.track_view_def_deleted(view)?;
		Ok(())
	}

	#[instrument(name = "catalog::view::list_all", level = "debug", skip(self, txn))]
	pub fn list_views_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<ViewDef>> {
		CatalogStore::list_views_all(txn)
	}

	#[instrument(name = "catalog::view::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_view_primary_key(
		&self,
		txn: &mut AdminTransaction,
		view_id: ViewId,
		primary_key_id: PrimaryKeyId,
	) -> Result<()> {
		CatalogStore::set_view_primary_key(txn, view_id, primary_key_id)
	}

	#[instrument(name = "catalog::view::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_view_pk_id(&self, txn: &mut Transaction<'_>, view_id: ViewId) -> Result<Option<PrimaryKeyId>> {
		CatalogStore::get_view_pk_id(txn, view_id)
	}
}
