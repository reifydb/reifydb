// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	column::Column,
	id::ColumnId,
	property::{ColumnProperty, ColumnPropertyKind},
	schema::SchemaId,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog, store::column::list::ColumnInfo};

impl Catalog {
	#[instrument(name = "catalog::column::find_by_name", level = "trace", skip(self, txn, schema, name))]
	pub fn find_column_by_name(
		&self,
		txn: &mut Transaction<'_>,
		schema: impl Into<SchemaId>,
		name: &str,
	) -> Result<Option<Column>> {
		CatalogStore::find_column_by_name(txn, schema, name)
	}

	#[instrument(name = "catalog::column::get", level = "trace", skip(self, txn))]
	pub fn get_column(&self, txn: &mut Transaction<'_>, column_id: ColumnId) -> Result<Column> {
		CatalogStore::get_column(txn, column_id)
	}

	#[instrument(name = "catalog::column::list_all", level = "debug", skip(self, txn))]
	pub fn list_columns_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<ColumnInfo>> {
		CatalogStore::list_columns_all(txn)
	}

	#[instrument(name = "catalog::column::create_policy", level = "debug", skip(self, txn))]
	pub fn create_column_property(
		&self,
		txn: &mut AdminTransaction,
		column: ColumnId,
		policy: ColumnPropertyKind,
	) -> Result<ColumnProperty> {
		CatalogStore::create_column_property(txn, column, policy)
	}
}
