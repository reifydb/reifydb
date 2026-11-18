// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::{CatalogTrackQueueChangeOperations, CatalogTrackViewChangeOperations},
		id::{NamespaceId, PrimaryKeyId, ViewId},
		queue::Queue,
		view::{View, ViewSortKey},
	},
	internal,
	row::row_shape_from_columns,
};
use reifydb_transaction::{
	change::TransactionalViewChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	error,
	error::ValueError,
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::{
		Catalog,
		name::{NameAllowed, QualifiedName},
	},
	error::NameKind,
	store::view::create::{
		ViewColumnToCreate as StoreViewColumnToCreate, ViewStorage, ViewToCreate as StoreViewToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<ViewColumnToCreate>,
	pub storage: ViewStorage,
	pub sort: Vec<ViewSortKey>,
}

impl ViewToCreate {
	pub fn qualified(&self) -> QualifiedName<'_> {
		QualifiedName {
			namespace: self.namespace,
			name: &self.name,
		}
	}
}

impl From<ViewColumnToCreate> for StoreViewColumnToCreate {
	fn from(col: ViewColumnToCreate) -> Self {
		StoreViewColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<ViewToCreate> for StoreViewToCreate {
	fn from(to_create: ViewToCreate) -> Self {
		StoreViewToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			storage: to_create.storage,
			sort: to_create.sort,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::view::find", level = "trace", skip(self, txn))]
	pub fn find_view(&self, txn: &mut Transaction<'_>, id: ViewId) -> Result<Option<View>, ValueError> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(view) = self.cache.find_view_at(id, cmd.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view(&mut Transaction::Command(&mut *cmd), id)? {
					warn!("View with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(view) = TransactionalViewChanges::find_view(admin, id) {
					return Ok(Some(view.clone()));
				}

				if TransactionalViewChanges::is_view_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(view) = self.cache.find_view_at(id, admin.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view(&mut Transaction::Admin(&mut *admin), id)? {
					warn!("View with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(view) = self.cache.find_view_at(id, qry.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view(&mut Transaction::Query(&mut *qry), id)? {
					warn!("View with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(view) = TransactionalViewChanges::find_view(t.inner, id) {
					return Ok(Some(view.clone()));
				}
				if TransactionalViewChanges::is_view_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(view) =
					CatalogStore::find_view(&mut Transaction::Test(Box::new(t.reborrow())), id)?
				{
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
	) -> Result<Option<View>, ValueError> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(view) = self.cache.find_view_by_name_at(namespace, name, cmd.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(view) = TransactionalViewChanges::find_view_by_name(admin, namespace, name)
				{
					return Ok(Some(view.clone()));
				}

				if TransactionalViewChanges::is_view_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(view) = self.cache.find_view_by_name_at(namespace, name, admin.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(view) = self.cache.find_view_by_name_at(namespace, name, qry.version()) {
					return Ok(Some(view));
				}

				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"View '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(view));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(view) =
					TransactionalViewChanges::find_view_by_name(t.inner, namespace, name)
				{
					return Ok(Some(view.clone()));
				}
				if TransactionalViewChanges::is_view_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(view) = CatalogStore::find_view_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(view));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::view::get", level = "trace", skip(self, txn))]
	pub fn get_view(&self, txn: &mut Transaction<'_>, id: ViewId) -> Result<View, ValueError> {
		self.find_view(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::view::create_deferred", level = "info", skip(self, txn, to_create))]
	pub fn create_deferred_view(
		&self,
		txn: &mut AdminTransaction,
		to_create: ViewToCreate,
	) -> Result<View, ValueError> {
		let owned_queue = match &to_create.storage {
			ViewStorage::Queue {
				queue,
				..
			} => Some(*queue),
			_ => None,
		};
		self.reject_name_taken_allowing(
			&mut Transaction::Admin(&mut *txn),
			to_create.qualified(),
			NameKind::View,
			NameAllowed {
				queue: owned_queue,
				..NameAllowed::default()
			},
		)?;
		let view = CatalogStore::create_deferred_view(txn, to_create.into())?;
		txn.track_view_created(view.clone())?;

		let family = view.storage_kind().row_family();
		let shape = row_shape_from_columns(family, view.columns());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), family, shape.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::create_transactional", level = "info", skip(self, txn, to_create))]
	pub fn create_transactional_view(
		&self,
		txn: &mut AdminTransaction,
		to_create: ViewToCreate,
	) -> Result<View, ValueError> {
		let owned_queue = match &to_create.storage {
			ViewStorage::Queue {
				queue,
				..
			} => Some(*queue),
			_ => None,
		};
		self.reject_name_taken_allowing(
			&mut Transaction::Admin(&mut *txn),
			to_create.qualified(),
			NameKind::View,
			NameAllowed {
				queue: owned_queue,
				..NameAllowed::default()
			},
		)?;
		let view = CatalogStore::create_transactional_view(txn, to_create.into())?;
		txn.track_view_created(view.clone())?;

		let family = view.storage_kind().row_family();
		let shape = row_shape_from_columns(family, view.columns());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), family, shape.fields().to_vec())?;

		Ok(view)
	}

	#[instrument(name = "catalog::view::drop", level = "info", skip(self, txn))]
	pub fn drop_view(&self, txn: &mut AdminTransaction, view: View) -> Result<(), ValueError> {
		let owned_queue: Option<Queue> = match &view {
			View::Queue(queue_view) => {
				Some(self.get_queue(&mut Transaction::Admin(&mut *txn), queue_view.queue)?)
			}
			_ => None,
		};
		CatalogStore::drop_view(txn, view.id())?;
		if let Some(queue) = owned_queue {
			txn.track_queue_deleted(queue)?;
		}
		txn.track_view_deleted(view)?;
		Ok(())
	}

	#[instrument(name = "catalog::view::list_all", level = "trace", skip(self, txn))]
	pub fn list_views_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<View>, ValueError> {
		CatalogStore::list_views_all(txn)
	}

	#[instrument(name = "catalog::view::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_view_pk_id(
		&self,
		txn: &mut Transaction<'_>,
		view_id: ViewId,
	) -> Result<Option<PrimaryKeyId>, ValueError> {
		CatalogStore::get_view_pk_id(txn, view_id)
	}
}
