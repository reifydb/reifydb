// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowFamily;
use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		change::CatalogTrackQueueChangeOperations,
		id::{ColumnId, NamespaceId, QueueId},
		property::ColumnPropertyKind,
		queue::{Queue, QueueDeduplicate, QueueDispatch, QueueRetention, QueueRetry},
	},
	internal,
	row::row_shape_from_columns,
};
use reifydb_transaction::{
	change::TransactionalQueueChanges,
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
	store::queue::create::{QueueColumnToCreate as StoreQueueColumnToCreate, QueueToCreate as StoreQueueToCreate},
};

#[derive(Debug, Clone)]
pub struct QueueColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct QueueToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<QueueColumnToCreate>,
	pub dispatch: QueueDispatch,
	pub deduplicate: Option<QueueDeduplicate>,
	pub retention: QueueRetention,
	pub retry: QueueRetry,
	pub underlying: bool,
	pub time: TimeSource,
}

impl From<QueueColumnToCreate> for StoreQueueColumnToCreate {
	fn from(col: QueueColumnToCreate) -> Self {
		StoreQueueColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
			auto_increment: col.auto_increment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<QueueToCreate> for StoreQueueToCreate {
	fn from(to_create: QueueToCreate) -> Self {
		StoreQueueToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			dispatch: to_create.dispatch,
			deduplicate: to_create.deduplicate,
			retention: to_create.retention,
			retry: to_create.retry,
			underlying: to_create.underlying,
			time: to_create.time,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::queue::find", level = "trace", skip(self, txn))]
	pub fn find_queue(&self, txn: &mut Transaction<'_>, id: QueueId) -> Result<Option<Queue>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(queue) = self.cache.find_queue_at(id, cmd.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("Queue {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(queue) = TransactionalQueueChanges::find_queue(admin, id) {
					return Ok(Some(queue.clone()));
				}
				if TransactionalQueueChanges::is_queue_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(queue) = self.cache.find_queue_at(id, admin.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("Queue {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(queue) = self.cache.find_queue_at(id, qry.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue(&mut Transaction::Query(&mut *qry), id)? {
					warn!("Queue {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(queue) = TransactionalQueueChanges::find_queue(t.inner, id) {
					return Ok(Some(queue.clone()));
				}
				if TransactionalQueueChanges::is_queue_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(queue) =
					CatalogStore::find_queue(&mut Transaction::Admin(&mut *t.inner), id)?
				{
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(queue) = self.cache.find_queue_at(id, rep.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue(&mut Transaction::Replica(&mut *rep), id)?
				{
					warn!("Queue {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(queue));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::queue::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_queue_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Queue>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(queue) = self.cache.find_queue_by_name_at(namespace, name, cmd.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Queue '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(queue) =
					TransactionalQueueChanges::find_queue_by_name(admin, namespace, name)
				{
					return Ok(Some(queue.clone()));
				}
				if TransactionalQueueChanges::is_queue_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}
				if let Some(queue) = self.cache.find_queue_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Queue '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(queue) = self.cache.find_queue_by_name_at(namespace, name, qry.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
					warn!(
						"Queue '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(queue) =
					TransactionalQueueChanges::find_queue_by_name(t.inner, namespace, name)
				{
					return Ok(Some(queue.clone()));
				}
				if TransactionalQueueChanges::is_queue_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(queue) = CatalogStore::find_queue_by_name(
					&mut Transaction::Admin(&mut *t.inner),
					namespace,
					name,
				)? {
					return Ok(Some(queue));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(queue) = self.cache.find_queue_by_name_at(namespace, name, rep.version()) {
					return Ok(Some(queue));
				}
				if let Some(queue) = CatalogStore::find_queue_by_name(
					&mut Transaction::Replica(&mut *rep),
					namespace,
					name,
				)? {
					warn!(
						"Queue '{}' in namespace {:?} found in storage but not in CatalogCache",
						name, namespace
					);
					return Ok(Some(queue));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::queue::get", level = "trace", skip(self, txn))]
	pub fn get_queue(&self, txn: &mut Transaction<'_>, id: QueueId) -> Result<Queue> {
		self.find_queue(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Queue with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::queue::create", level = "info", skip(self, txn, to_create))]
	pub fn create_queue(&self, txn: &mut AdminTransaction, to_create: QueueToCreate) -> Result<Queue> {
		let queue = CatalogStore::create_queue(txn, to_create.into())?;
		txn.track_queue_created(queue.clone())?;

		let shape = row_shape_from_columns(queue.columns.as_slice());
		self.get_or_create_row_shape(
			&mut Transaction::Admin(&mut *txn),
			RowFamily::Queue,
			shape.fields().to_vec(),
		)?;

		Ok(queue)
	}

	pub fn create_queue_with_id(
		&self,
		txn: &mut AdminTransaction,
		queue_id: QueueId,
		to_create: QueueToCreate,
		column_ids: &[ColumnId],
	) -> Result<Queue> {
		let queue = CatalogStore::create_queue_with_id(txn, queue_id, to_create.into(), column_ids)?;
		txn.track_queue_created(queue.clone())?;

		let shape = row_shape_from_columns(queue.columns.as_slice());
		self.get_or_create_row_shape(
			&mut Transaction::Admin(&mut *txn),
			RowFamily::Queue,
			shape.fields().to_vec(),
		)?;

		Ok(queue)
	}

	#[instrument(name = "catalog::queue::drop", level = "info", skip(self, txn))]
	pub fn drop_queue(&self, txn: &mut AdminTransaction, queue: Queue) -> Result<()> {
		CatalogStore::drop_queue(txn, queue.id)?;
		txn.track_queue_deleted(queue)?;
		Ok(())
	}

	#[instrument(name = "catalog::queue::list_all", level = "trace", skip(self, txn))]
	pub fn list_queues_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Queue>> {
		CatalogStore::list_queues_all(txn)
	}

	#[instrument(name = "catalog::queue::list", level = "trace", skip(self, txn))]
	pub fn list_queues(&self, txn: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<Queue>> {
		CatalogStore::list_queues(txn, namespace)
	}
}
