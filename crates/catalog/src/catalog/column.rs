// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	column::Column,
	id::ColumnId,
	object::ObjectId,
	property::{ColumnProperty, ColumnPropertyKind},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog, store::column::list::ColumnInfo};

impl Catalog {
	#[instrument(name = "catalog::column::find_by_name", level = "trace", skip(self, txn, object, name))]
	pub fn find_column_by_name(
		&self,
		txn: &mut Transaction<'_>,
		object: impl Into<ObjectId>,
		name: &str,
	) -> Result<Option<Column>> {
		CatalogStore::find_column_by_name(txn, object, name)
	}

	#[instrument(name = "catalog::column::get", level = "trace", skip(self, txn))]
	pub fn get_column(&self, txn: &mut Transaction<'_>, column_id: ColumnId) -> Result<Column> {
		CatalogStore::get_column(txn, column_id)
	}

	#[instrument(name = "catalog::column::list_all", level = "trace", skip(self, txn))]
	pub fn list_columns_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<ColumnInfo>> {
		CatalogStore::list_columns_all(txn)
	}

	#[instrument(name = "catalog::column::create_policy", level = "info", skip(self, txn))]
	pub fn create_column_property(
		&self,
		txn: &mut AdminTransaction,
		column: ColumnId,
		policy: ColumnPropertyKind,
	) -> Result<ColumnProperty> {
		CatalogStore::create_column_property(txn, column, policy)
	}
}
