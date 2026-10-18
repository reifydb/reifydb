// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackColumnBlockChangeOperations,
	column_block::ColumnBlock,
	id::{ColumnBlockId, SeriesId, TableId},
};
use reifydb_transaction::{
	change::TransactionalColumnBlockChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::partition::Partition;
use tracing::{instrument, warn};

use crate::{CatalogStore, Result, catalog::Catalog, store::column_block::create::ColumnBlockToCreate};

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
