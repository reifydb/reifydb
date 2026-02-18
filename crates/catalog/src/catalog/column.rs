// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{column::ColumnDef, id::ColumnId, primitive::PrimitiveId};
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog, store::column::list::ColumnInfo};

impl Catalog {
	#[instrument(name = "catalog::column::find_by_name", level = "trace", skip(self, txn, source, name))]
	pub fn find_column_by_name(
		&self,
		txn: &mut Transaction<'_>,
		source: impl Into<PrimitiveId>,
		name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		CatalogStore::find_column_by_name(txn, source, name)
	}

	#[instrument(name = "catalog::column::get", level = "trace", skip(self, txn))]
	pub fn get_column(&self, txn: &mut Transaction<'_>, column_id: ColumnId) -> crate::Result<ColumnDef> {
		CatalogStore::get_column(txn, column_id)
	}

	#[instrument(name = "catalog::column::list_all", level = "debug", skip(self, txn))]
	pub fn list_columns_all(&self, txn: &mut Transaction<'_>) -> crate::Result<Vec<ColumnInfo>> {
		CatalogStore::list_columns_all(txn)
	}
}
