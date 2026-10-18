// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackColumnBlockChangeOperations,
	column_block::{ColumnBlock, ColumnBlockSource},
	id::{ColumnBlockId, SeriesId, TableId},
};
use reifydb_transaction::{
	change::{OperationType, TransactionalColumnBlockChanges},
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::partition::Partition;
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::column_block::{create::ColumnBlockToCreate, update::ColumnBlockToUpdate},
};

impl Catalog {
	#[instrument(name = "catalog::column_block::find", level = "trace", skip(self, txn))]
	pub fn find_column_block(&self, txn: &mut Transaction<'_>, id: ColumnBlockId) -> Result<Option<ColumnBlock>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(block) = self.cache.find_column_block_at(id, cmd.version()) {
					return Ok(Some(block));
				}
				if let Some(block) =
					CatalogStore::find_column_block(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("ColumnBlock with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(block));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(block) = TransactionalColumnBlockChanges::find_column_block(admin, id) {
					return Ok(Some(block.clone()));
				}
				if TransactionalColumnBlockChanges::is_column_block_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(block) = self.cache.find_column_block_at(id, admin.version()) {
					return Ok(Some(block));
				}
				if let Some(block) =
					CatalogStore::find_column_block(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("ColumnBlock with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(block));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(block) = self.cache.find_column_block_at(id, qry.version()) {
					return Ok(Some(block));
				}
				if let Some(block) =
					CatalogStore::find_column_block(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!("ColumnBlock with ID {:?} found in storage but not in CatalogCache", id);
					return Ok(Some(block));
				}
				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(block) = TransactionalColumnBlockChanges::find_column_block(t.inner, id) {
					return Ok(Some(block.clone()));
				}
				if TransactionalColumnBlockChanges::is_column_block_deleted(t.inner, id) {
					return Ok(None);
				}
				CatalogStore::find_column_block(&mut Transaction::Test(Box::new(t.reborrow())), id)
			}
		}
	}

	#[instrument(name = "catalog::column_block::find_for_series_bucket", level = "trace", skip(self, txn))]
	pub fn find_column_block_for_series_bucket(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Option<Partition>,
		bucket_start: u64,
	) -> Result<Option<ColumnBlock>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self
				.cache
				.find_column_block_for_series_bucket_at(
					series_id,
					partition,
					bucket_start,
					cmd.version(),
				)
				.or(CatalogStore::find_column_block_for_series_bucket(
					&mut Transaction::Command(&mut *cmd),
					series_id,
					partition,
					bucket_start,
				)?)),
			Transaction::Admin(admin) => {
				for change in admin.changes.column_block.iter().rev() {
					if let Some(block) = &change.post
						&& let ColumnBlockSource::SeriesBucket {
							series_id: sid,
							bucket_start: bs,
							partition: p,
							..
						} = block.source && sid == series_id && bs == bucket_start
						&& p == partition
					{
						return Ok(Some(block.clone()));
					} else if let Some(block) = &change.pre
						&& let ColumnBlockSource::SeriesBucket {
							series_id: sid,
							bucket_start: bs,
							partition: p,
							..
						} = block.source && sid == series_id && bs == bucket_start
						&& p == partition && change.op == OperationType::Delete
					{
						return Ok(None);
					}
				}
				if let Some(block) = self.cache.find_column_block_for_series_bucket_at(
					series_id,
					partition,
					bucket_start,
					admin.version(),
				) {
					return Ok(Some(block));
				}
				CatalogStore::find_column_block_for_series_bucket(
					&mut Transaction::Admin(&mut *admin),
					series_id,
					partition,
					bucket_start,
				)
			}
			Transaction::Query(qry) => Ok(self
				.cache
				.find_column_block_for_series_bucket_at(
					series_id,
					partition,
					bucket_start,
					qry.version(),
				)
				.or(CatalogStore::find_column_block_for_series_bucket(
					&mut Transaction::Query(&mut *qry),
					series_id,
					partition,
					bucket_start,
				)?)),
			Transaction::Test(mut t) => CatalogStore::find_column_block_for_series_bucket(
				&mut Transaction::Test(Box::new(t.reborrow())),
				series_id,
				partition,
				bucket_start,
			),
		}
	}

	#[instrument(name = "catalog::column_block::find_latest_for_table", level = "trace", skip(self, txn))]
	pub fn find_latest_column_block_for_table(
		&self,
		txn: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Option<ColumnBlock>> {
		let version = match txn.reborrow() {
			Transaction::Command(cmd) => cmd.version(),
			Transaction::Admin(admin) => admin.version(),
			Transaction::Query(qry) => qry.version(),
			Transaction::Test(_) => {
				return CatalogStore::find_latest_column_block_for_table(txn, table_id);
			}
		};
		if let Some(block) = self.cache.find_latest_column_block_for_table_at(table_id, version) {
			return Ok(Some(block));
		}
		CatalogStore::find_latest_column_block_for_table(txn, table_id)
	}

	#[instrument(name = "catalog::column_block::list_for_series", level = "trace", skip(self, txn))]
	pub fn list_column_blocks_for_series(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Vec<ColumnBlock>> {
		let version_opt = match txn.reborrow() {
			Transaction::Command(cmd) => Some(cmd.version()),
			Transaction::Admin(admin) => Some(admin.version()),
			Transaction::Query(qry) => Some(qry.version()),
			Transaction::Test(_) => None,
		};
		if let Some(version) = version_opt {
			let cached = self.cache.list_column_blocks_for_series_at(series_id, version);
			if !cached.is_empty() {
				return Ok(cached);
			}
		}
		CatalogStore::list_column_blocks_for_series(txn, series_id)
	}

	#[instrument(name = "catalog::column_block::list_for_series_partition", level = "trace", skip(self, txn))]
	pub fn list_column_blocks_for_series_partition(
		&self,
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Partition,
	) -> Result<Vec<ColumnBlock>> {
		let version_opt = match txn.reborrow() {
			Transaction::Command(cmd) => Some(cmd.version()),
			Transaction::Admin(admin) => Some(admin.version()),
			Transaction::Query(qry) => Some(qry.version()),
			Transaction::Test(_) => None,
		};
		if let Some(version) = version_opt {
			let cached =
				self.cache.list_column_blocks_for_series_partition_at(series_id, partition, version);
			if !cached.is_empty() {
				return Ok(cached);
			}
		}
		CatalogStore::list_column_blocks_for_series_partition(txn, series_id, partition)
	}

	#[instrument(name = "catalog::column_block::list_for_table", level = "trace", skip(self, txn))]
	pub fn list_column_blocks_for_table(
		&self,
		txn: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Vec<ColumnBlock>> {
		let version_opt = match txn.reborrow() {
			Transaction::Command(cmd) => Some(cmd.version()),
			Transaction::Admin(admin) => Some(admin.version()),
			Transaction::Query(qry) => Some(qry.version()),
			Transaction::Test(_) => None,
		};
		if let Some(version) = version_opt {
			let cached = self.cache.list_column_blocks_for_table_at(table_id, version);
			if !cached.is_empty() {
				return Ok(cached);
			}
		}
		CatalogStore::list_column_blocks_for_table(txn, table_id)
	}

	#[instrument(name = "catalog::column_block::create", level = "info", skip(self, txn, to_create))]
	pub fn create_column_block(
		&self,
		txn: &mut AdminTransaction,
		to_create: ColumnBlockToCreate,
	) -> Result<ColumnBlock> {
		let block = CatalogStore::create_column_block(txn, to_create)?;
		txn.track_column_block_created(block.clone())?;
		Ok(block)
	}

	#[instrument(name = "catalog::column_block::update", level = "debug", skip(self, txn, patch))]
	pub fn update_column_block(
		&self,
		txn: &mut AdminTransaction,
		id: ColumnBlockId,
		patch: ColumnBlockToUpdate,
	) -> Result<ColumnBlock> {
		let pre = CatalogStore::get_column_block(&mut Transaction::Admin(&mut *txn), id)?;
		let post = CatalogStore::update_column_block(txn, id, patch)?;
		txn.track_column_block_updated(pre, post.clone())?;
		Ok(post)
	}

	#[instrument(name = "catalog::column_block::drop", level = "info", skip(self, txn))]
	pub fn drop_column_block(&self, txn: &mut AdminTransaction, id: ColumnBlockId) -> Result<()> {
		let pre = CatalogStore::find_column_block(&mut Transaction::Admin(&mut *txn), id)?;
		CatalogStore::drop_column_block(txn, id)?;
		if let Some(block) = pre {
			txn.track_column_block_deleted(block)?;
		}
		Ok(())
	}
}
