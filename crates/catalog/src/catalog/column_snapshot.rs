// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackColumnSnapshotChangeOperations,
	column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
	id::{ColumnSnapshotId, SeriesId, TableId},
};
use reifydb_transaction::{
	change::{OperationType, TransactionalColumnSnapshotChanges},
	transaction::{Transaction, admin::AdminTransaction},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::column_snapshot::{create::ColumnSnapshotToCreate, update::ColumnSnapshotToUpdate},
};

impl Catalog {
	#[instrument(name = "catalog::column_snapshot::find", level = "trace", skip(self, txn))]
	pub fn find_column_snapshot(
		&self,
		txn: &mut Transaction<'_>,
		id: ColumnSnapshotId,
	) -> Result<Option<ColumnSnapshot>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(snap) = self.cache.find_column_snapshot_at(id, cmd.version()) {
					return Ok(Some(snap));
				}
				if let Some(snap) =
					CatalogStore::find_column_snapshot(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"ColumnSnapshot with ID {:?} found in storage but not in CatalogCache",
						id
					);
					return Ok(Some(snap));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(snap) = TransactionalColumnSnapshotChanges::find_column_snapshot(admin, id)
				{
					return Ok(Some(snap.clone()));
				}
				if TransactionalColumnSnapshotChanges::is_column_snapshot_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(snap) = self.cache.find_column_snapshot_at(id, admin.version()) {
					return Ok(Some(snap));
				}
				if let Some(snap) =
					CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"ColumnSnapshot with ID {:?} found in storage but not in CatalogCache",
						id
					);
					return Ok(Some(snap));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(snap) = self.cache.find_column_snapshot_at(id, qry.version()) {
					return Ok(Some(snap));
				}
				if let Some(snap) =
					CatalogStore::find_column_snapshot(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!(
						"ColumnSnapshot with ID {:?} found in storage but not in CatalogCache",
						id
					);
					return Ok(Some(snap));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(snap) =
					TransactionalColumnSnapshotChanges::find_column_snapshot(t.inner, id)
				{
					return Ok(Some(snap.clone()));
				}
				if TransactionalColumnSnapshotChanges::is_column_snapshot_deleted(t.inner, id) {
					return Ok(None);
				}
				CatalogStore::find_column_snapshot(&mut Transaction::Test(Box::new(t.reborrow())), id)
			}
			Transaction::Replica(rep) => {
				if let Some(snap) = self.cache.find_column_snapshot_at(id, rep.version()) {
					return Ok(Some(snap));
				}
				CatalogStore::find_column_snapshot(&mut Transaction::Replica(&mut *rep), id)
			}
		}
	}

	#[instrument(name = "catalog::column_snapshot::find_for_series_bucket", level = "trace", skip(self, txn))]
	pub fn find_column_snapshot_for_series_bucket(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		bucket_start: u64,
	) -> Result<Option<ColumnSnapshot>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self
				.cache
				.find_column_snapshot_for_series_bucket_at(series_id, bucket_start, cmd.version())
				.or(CatalogStore::find_column_snapshot_for_series_bucket(
					&mut Transaction::Command(&mut *cmd),
					series_id,
					bucket_start,
				)?)),
			Transaction::Admin(admin) => {
				for change in admin.changes.column_snapshot.iter().rev() {
					if let Some(snap) = &change.post
						&& let ColumnSnapshotSource::SeriesBucket {
							series_id: sid,
							bucket_start: bs,
							..
						} = snap.source && sid == series_id && bs == bucket_start
					{
						return Ok(Some(snap.clone()));
					} else if let Some(snap) = &change.pre
						&& let ColumnSnapshotSource::SeriesBucket {
							series_id: sid,
							bucket_start: bs,
							..
						} = snap.source && sid == series_id && bs == bucket_start
						&& change.op == OperationType::Delete
					{
						return Ok(None);
					}
				}
				if let Some(snap) = self.cache.find_column_snapshot_for_series_bucket_at(
					series_id,
					bucket_start,
					admin.version(),
				) {
					return Ok(Some(snap));
				}
				CatalogStore::find_column_snapshot_for_series_bucket(
					&mut Transaction::Admin(&mut *admin),
					series_id,
					bucket_start,
				)
			}
			Transaction::Query(qry) => Ok(self
				.cache
				.find_column_snapshot_for_series_bucket_at(series_id, bucket_start, qry.version())
				.or(CatalogStore::find_column_snapshot_for_series_bucket(
					&mut Transaction::Query(&mut *qry),
					series_id,
					bucket_start,
				)?)),
			Transaction::Test(mut t) => CatalogStore::find_column_snapshot_for_series_bucket(
				&mut Transaction::Test(Box::new(t.reborrow())),
				series_id,
				bucket_start,
			),
			Transaction::Replica(rep) => Ok(self
				.cache
				.find_column_snapshot_for_series_bucket_at(series_id, bucket_start, rep.version())
				.or(CatalogStore::find_column_snapshot_for_series_bucket(
					&mut Transaction::Replica(&mut *rep),
					series_id,
					bucket_start,
				)?)),
		}
	}

	#[instrument(name = "catalog::column_snapshot::find_latest_for_table", level = "trace", skip(self, txn))]
	pub fn find_latest_column_snapshot_for_table(
		&self,
		txn: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Option<ColumnSnapshot>> {
		let version = match txn.reborrow() {
			Transaction::Command(cmd) => cmd.version(),
			Transaction::Admin(admin) => admin.version(),
			Transaction::Query(qry) => qry.version(),
			Transaction::Replica(rep) => rep.version(),
			Transaction::Test(_) => {
				return CatalogStore::find_latest_column_snapshot_for_table(txn, table_id);
			}
		};
		if let Some(snap) = self.cache.find_latest_column_snapshot_for_table_at(table_id, version) {
			return Ok(Some(snap));
		}
		CatalogStore::find_latest_column_snapshot_for_table(txn, table_id)
	}

	#[instrument(name = "catalog::column_snapshot::list_for_series", level = "trace", skip(self, txn))]
	pub fn list_column_snapshots_for_series(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Vec<ColumnSnapshot>> {
		let version_opt = match txn.reborrow() {
			Transaction::Command(cmd) => Some(cmd.version()),
			Transaction::Admin(admin) => Some(admin.version()),
			Transaction::Query(qry) => Some(qry.version()),
			Transaction::Replica(rep) => Some(rep.version()),
			Transaction::Test(_) => None,
		};
		if let Some(version) = version_opt {
			let cached = self.cache.list_column_snapshots_for_series_at(series_id, version);
			if !cached.is_empty() {
				return Ok(cached);
			}
		}
		CatalogStore::list_column_snapshots_for_series(txn, series_id)
	}

	#[instrument(name = "catalog::column_snapshot::list_for_table", level = "trace", skip(self, txn))]
	pub fn list_column_snapshots_for_table(
		&self,
		txn: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Vec<ColumnSnapshot>> {
		let version_opt = match txn.reborrow() {
			Transaction::Command(cmd) => Some(cmd.version()),
			Transaction::Admin(admin) => Some(admin.version()),
			Transaction::Query(qry) => Some(qry.version()),
			Transaction::Replica(rep) => Some(rep.version()),
			Transaction::Test(_) => None,
		};
		if let Some(version) = version_opt {
			let cached = self.cache.list_column_snapshots_for_table_at(table_id, version);
			if !cached.is_empty() {
				return Ok(cached);
			}
		}
		CatalogStore::list_column_snapshots_for_table(txn, table_id)
	}

	#[instrument(name = "catalog::column_snapshot::create", level = "info", skip(self, txn, to_create))]
	pub fn create_column_snapshot(
		&self,
		txn: &mut AdminTransaction,
		to_create: ColumnSnapshotToCreate,
	) -> Result<ColumnSnapshot> {
		let snapshot = CatalogStore::create_column_snapshot(txn, to_create)?;
		txn.track_column_snapshot_created(snapshot.clone())?;
		Ok(snapshot)
	}

	#[instrument(name = "catalog::column_snapshot::update", level = "debug", skip(self, txn, patch))]
	pub fn update_column_snapshot(
		&self,
		txn: &mut AdminTransaction,
		id: ColumnSnapshotId,
		patch: ColumnSnapshotToUpdate,
	) -> Result<ColumnSnapshot> {
		let pre = CatalogStore::get_column_snapshot(&mut Transaction::Admin(&mut *txn), id)?;
		let post = CatalogStore::update_column_snapshot(txn, id, patch)?;
		txn.track_column_snapshot_updated(pre, post.clone())?;
		Ok(post)
	}

	#[instrument(name = "catalog::column_snapshot::drop", level = "info", skip(self, txn))]
	pub fn drop_column_snapshot(&self, txn: &mut AdminTransaction, id: ColumnSnapshotId) -> Result<()> {
		let pre = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut *txn), id)?;
		CatalogStore::drop_column_snapshot(txn, id)?;
		if let Some(snap) = pre {
			txn.track_column_snapshot_deleted(snap)?;
		}
		Ok(())
	}
}
