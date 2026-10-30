// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackWindowedViewGroupChangeOperations,
	flow::FlowId,
	id::{NamespaceId, ViewId},
	windowed_view_group::WindowedViewGroup,
};
use reifydb_transaction::{
	change::TransactionalWindowedViewGroupChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::fragment::Fragment;
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result, catalog::Catalog,
	store::windowed_view_group::create::WindowedViewGroupToCreate as StoreWindowedViewGroupToCreate,
};

#[derive(Debug, Clone)]
pub struct WindowedViewGroupToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub flow: FlowId,
	pub members: Vec<ViewId>,
}

impl From<WindowedViewGroupToCreate> for StoreWindowedViewGroupToCreate {
	fn from(to_create: WindowedViewGroupToCreate) -> Self {
		StoreWindowedViewGroupToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			flow: to_create.flow,
			members: to_create.members,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::windowed_view_group::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_windowed_view_group_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<WindowedViewGroup>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(group) =
					self.cache.find_windowed_view_group_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(group));
				}
				if let Some(group) = CatalogStore::find_windowed_view_group_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"WindowedViewGroup '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(group));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(group) =
					TransactionalWindowedViewGroupChanges::find_windowed_view_group_by_name(
						admin, namespace, name,
					) {
					return Ok(Some(group.clone()));
				}
				if TransactionalWindowedViewGroupChanges::is_windowed_view_group_deleted_by_name(
					admin, namespace, name,
				) {
					return Ok(None);
				}
				if let Some(group) =
					self.cache.find_windowed_view_group_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(group));
				}
				if let Some(group) = CatalogStore::find_windowed_view_group_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"WindowedViewGroup '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(group));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(group) =
					self.cache.find_windowed_view_group_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(group));
				}
				if let Some(group) = CatalogStore::find_windowed_view_group_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"WindowedViewGroup '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(group));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(group) =
					TransactionalWindowedViewGroupChanges::find_windowed_view_group_by_name(
						t.inner, namespace, name,
					) {
					return Ok(Some(group.clone()));
				}
				if TransactionalWindowedViewGroupChanges::is_windowed_view_group_deleted_by_name(
					t.inner, namespace, name,
				) {
					return Ok(None);
				}
				if let Some(group) = CatalogStore::find_windowed_view_group_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(group));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::windowed_view_group::create", level = "info", skip(self, txn, to_create))]
	pub fn create_windowed_view_group(
		&self,
		txn: &mut AdminTransaction,
		to_create: WindowedViewGroupToCreate,
	) -> Result<WindowedViewGroup> {
		let group = CatalogStore::create_windowed_view_group(txn, to_create.into())?;
		txn.track_windowed_view_group_created(group.clone())?;
		Ok(group)
	}

	#[instrument(name = "catalog::windowed_view_group::drop", level = "info", skip(self, txn))]
	pub fn drop_windowed_view_group(&self, txn: &mut AdminTransaction, group: WindowedViewGroup) -> Result<()> {
		CatalogStore::drop_windowed_view_group(txn, group.id)?;
		txn.track_windowed_view_group_deleted(group)?;
		Ok(())
	}

	#[instrument(name = "catalog::windowed_view_group::list_all", level = "trace", skip(self, txn))]
	pub fn list_windowed_view_groups_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<WindowedViewGroup>> {
		CatalogStore::list_windowed_view_groups_all(txn)
	}
}
