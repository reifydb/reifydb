// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackSegmentTreeChangeOperations,
		id::{ColumnId, NamespaceId, SegmentTreeId},
		key::KeySpec,
		property::ColumnPropertyKind,
		segment_tree::{SegmentTree, SegmentTreeAggregate, SegmentTreeMetadata},
	},
	internal,
	row::row_shape_from_columns,
};
use reifydb_transaction::{
	change::TransactionalSegmentTreeChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::segment_tree::create::{
		SegmentTreeColumnToCreate as StoreSegmentTreeColumnToCreate,
		SegmentTreeToCreate as StoreSegmentTreeToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct SegmentTreeColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SegmentTreeToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SegmentTreeColumnToCreate>,
	pub key: KeySpec,
	pub aggregates: Vec<SegmentTreeAggregate>,
	pub partition_by: Vec<String>,
	pub underlying: bool,
}

impl From<SegmentTreeColumnToCreate> for StoreSegmentTreeColumnToCreate {
	fn from(col: SegmentTreeColumnToCreate) -> Self {
		StoreSegmentTreeColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
			auto_increment: col.auto_increment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<SegmentTreeToCreate> for StoreSegmentTreeToCreate {
	fn from(to_create: SegmentTreeToCreate) -> Self {
		StoreSegmentTreeToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			key: to_create.key,
			aggregates: to_create.aggregates,
			partition_by: to_create.partition_by,
			underlying: to_create.underlying,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::segment_tree::find", level = "trace", skip(self, txn))]
	pub fn find_segment_tree(&self, txn: &mut Transaction<'_>, id: SegmentTreeId) -> Result<Option<SegmentTree>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(segment_tree) = self.cache.find_segment_tree_at(id, cmd.version()) {
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) =
					CatalogStore::find_segment_tree(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("SegmentTree {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(segment_tree) =
					TransactionalSegmentTreeChanges::find_segment_tree(admin, id)
				{
					return Ok(Some(segment_tree.clone()));
				}
				if TransactionalSegmentTreeChanges::is_segment_tree_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(segment_tree) = self.cache.find_segment_tree_at(id, admin.version()) {
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) =
					CatalogStore::find_segment_tree(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("SegmentTree {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(segment_tree) = self.cache.find_segment_tree_at(id, qry.version()) {
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) =
					CatalogStore::find_segment_tree(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!("SegmentTree {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(segment_tree) =
					TransactionalSegmentTreeChanges::find_segment_tree(t.inner, id)
				{
					return Ok(Some(segment_tree.clone()));
				}
				if TransactionalSegmentTreeChanges::is_segment_tree_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(segment_tree) =
					CatalogStore::find_segment_tree(&mut Transaction::Admin(&mut *t.inner), id)?
				{
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(segment_tree) = self.cache.find_segment_tree_at(id, rep.version()) {
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) =
					CatalogStore::find_segment_tree(&mut Transaction::Replica(&mut *rep), id)?
				{
					warn!("SegmentTree {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::segment_tree::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_segment_tree_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<SegmentTree>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(segment_tree) =
					self.cache.find_segment_tree_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"SegmentTree '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(segment_tree) = TransactionalSegmentTreeChanges::find_segment_tree_by_name(
					admin, namespace, name,
				) {
					return Ok(Some(segment_tree.clone()));
				}
				if TransactionalSegmentTreeChanges::is_segment_tree_deleted_by_name(
					admin, namespace, name,
				) {
					return Ok(None);
				}
				if let Some(segment_tree) =
					self.cache.find_segment_tree_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"SegmentTree '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(segment_tree) =
					self.cache.find_segment_tree_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"SegmentTree '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(segment_tree) = TransactionalSegmentTreeChanges::find_segment_tree_by_name(
					t.inner, namespace, name,
				) {
					return Ok(Some(segment_tree.clone()));
				}
				if TransactionalSegmentTreeChanges::is_segment_tree_deleted_by_name(
					t.inner, namespace, name,
				) {
					return Ok(None);
				}
				if let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
					&mut Transaction::Admin(&mut *t.inner),
					namespace,
					name,
				)? {
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(segment_tree) =
					self.cache.find_segment_tree_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(segment_tree));
				}
				if let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
					&mut Transaction::Replica(&mut *rep),
					namespace,
					name,
				)? {
					warn!(
						"SegmentTree '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(segment_tree));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::segment_tree::get", level = "trace", skip(self, txn))]
	pub fn get_segment_tree(&self, txn: &mut Transaction<'_>, id: SegmentTreeId) -> Result<SegmentTree> {
		self.find_segment_tree(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"SegmentTree with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::segment_tree::create", level = "info", skip(self, txn, to_create))]
	pub fn create_segment_tree(
		&self,
		txn: &mut AdminTransaction,
		to_create: SegmentTreeToCreate,
	) -> Result<SegmentTree> {
		let segment_tree = CatalogStore::create_segment_tree(txn, to_create.into())?;
		txn.track_segment_tree_created(segment_tree.clone())?;

		let shape = row_shape_from_columns(segment_tree.columns.as_slice());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), shape.fields().to_vec())?;

		Ok(segment_tree)
	}

	pub fn create_segment_tree_with_id(
		&self,
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		to_create: SegmentTreeToCreate,
		column_ids: &[ColumnId],
	) -> Result<SegmentTree> {
		let segment_tree =
			CatalogStore::create_segment_tree_with_id(txn, segment_tree_id, to_create.into(), column_ids)?;
		txn.track_segment_tree_created(segment_tree.clone())?;

		let shape = row_shape_from_columns(segment_tree.columns.as_slice());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), shape.fields().to_vec())?;

		Ok(segment_tree)
	}

	#[instrument(name = "catalog::segment_tree::drop", level = "info", skip(self, txn))]
	pub fn drop_segment_tree(&self, txn: &mut AdminTransaction, segment_tree: SegmentTree) -> Result<()> {
		CatalogStore::drop_segment_tree(txn, segment_tree.id)?;
		txn.track_segment_tree_deleted(segment_tree)?;
		Ok(())
	}

	#[instrument(name = "catalog::segment_tree::list_all", level = "trace", skip(self, txn))]
	pub fn list_segment_tree_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<SegmentTree>> {
		CatalogStore::list_segment_tree_all(txn)
	}

	#[instrument(name = "catalog::segment_tree::find_metadata", level = "trace", skip(self, txn))]
	pub fn find_segment_tree_metadata(
		&self,
		txn: &mut Transaction<'_>,
		id: SegmentTreeId,
	) -> Result<Option<SegmentTreeMetadata>> {
		CatalogStore::find_segment_tree_metadata(txn, id)
	}

	#[instrument(name = "catalog::segment_tree::update_metadata_txn", level = "debug", skip(self, txn))]
	pub fn update_segment_tree_metadata_txn(
		&self,
		txn: &mut Transaction<'_>,
		metadata: SegmentTreeMetadata,
	) -> Result<()> {
		CatalogStore::update_segment_tree_metadata_txn(txn, metadata)
	}
}
