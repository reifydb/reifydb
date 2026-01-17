// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{column::ColumnDef, id::ColumnId, primitive::PrimitiveId};
use reifydb_transaction::standard::IntoStandardTransaction;
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog, store::column::list::ColumnInfo};

impl Catalog {
	#[instrument(name = "catalog::column::find_by_name", level = "trace", skip(self, txn, source, name))]
	pub fn find_column_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		source: impl Into<PrimitiveId>,
		name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		CatalogStore::find_column_by_name(txn, source, name)
	}

	#[instrument(name = "catalog::column::get", level = "trace", skip(self, txn))]
	pub fn get_column<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		column_id: ColumnId,
	) -> crate::Result<ColumnDef> {
		CatalogStore::get_column(txn, column_id)
	}

	#[instrument(name = "catalog::column::list_all", level = "debug", skip(self, txn))]
	pub fn list_columns_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<ColumnInfo>> {
		CatalogStore::list_columns_all(txn)
	}
}
