// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackTableChangeOperations,
		column::ColumnDef,
		id::{DictionaryId, NamespaceId, PrimaryKeyId, TableId},
		policy::ColumnPolicyKind,
		primitive::PrimitiveId,
		table::TableDef,
	},
	retention::RetentionPolicy,
};
use reifydb_transaction::{
	change::TransactionalTableChanges,
	standard::{IntoStandardTransaction, StandardTransaction, command::StandardCommandTransaction},
};
use reifydb_core::{error::diagnostic::catalog::table_not_found, internal};
use reifydb_type::{
	error, fragment::Fragment,
	value::constraint::TypeConstraint,
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	store::{
		primary_key::create::PrimaryKeyToCreate,
		table::create::{TableColumnToCreate as StoreTableColumnToCreate, TableToCreate as StoreTableToCreate},
	},
};

/// Column specification for table creation via Catalog API.
#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<Fragment>,
	pub dictionary_id: Option<DictionaryId>,
}

/// Table creation specification for the Catalog API.
///
/// This struct includes `primary_key_columns` which allows specifying primary key
/// column names at creation time. The Catalog will handle resolving column names
/// to IDs and creating the primary key record.
#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub fragment: Option<Fragment>,
	pub table: String,
	pub namespace: NamespaceId,
	pub columns: Vec<TableColumnToCreate>,
	pub retention_policy: Option<RetentionPolicy>,
	/// Optional primary key columns specified by name.
	/// If provided, the Catalog will create a primary key after creating the table.
	pub primary_key_columns: Option<Vec<String>>,
}

impl From<TableColumnToCreate> for StoreTableColumnToCreate {
	fn from(col: TableColumnToCreate) -> Self {
		StoreTableColumnToCreate {
			name: col.name,
			constraint: col.constraint,
			policies: col.policies,
			auto_increment: col.auto_increment,
			fragment: col.fragment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<TableToCreate> for StoreTableToCreate {
	fn from(to_create: TableToCreate) -> Self {
		StoreTableToCreate {
			fragment: to_create.fragment,
			table: to_create.table,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			retention_policy: to_create.retention_policy,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::table::find", level = "trace", skip(self, txn))]
	pub fn find_table<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: TableId,
	) -> crate::Result<Option<TableDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(table) = TransactionalTableChanges::find_table(cmd, id) {
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted(cmd, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) = self.materialized.find_table_at(id, cmd.version()) {
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(cmd, id)? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) = self.materialized.find_table_at(id, qry.version()) {
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(qry, id)? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::table::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_table_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<TableDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check transactional changes first
				if let Some(table) = TransactionalTableChanges::find_table_by_name(cmd, namespace, name)
				{
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted_by_name(cmd, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(cmd, namespace, name)? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(qry, namespace, name)? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::table::get", level = "trace", skip(self, txn))]
	pub fn get_table<T: IntoStandardTransaction>(&self, txn: &mut T, id: TableId) -> crate::Result<TableDef> {
		self.find_table(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::table::get_by_name", level = "trace", skip(self, txn, name))]
	pub fn get_table_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<TableDef> {
		let name = name.into();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(txn, namespace)?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_table_by_name(txn, namespace, name.text())?
			.ok_or_else(|| error!(table_not_found(name.clone(), &namespace_name, name.text())))
	}

	#[instrument(name = "catalog::table::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_table(
		&self,
		txn: &mut StandardCommandTransaction,
		to_create: TableToCreate,
	) -> crate::Result<TableDef> {
		let pk_columns = to_create.primary_key_columns.clone();

		let table = CatalogStore::create_table(txn, to_create.into())?;
		txn.track_table_def_created(table.clone())?;

		let schema = Schema::from(table.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		if let Some(pk_columns) = pk_columns {
			let table_columns = CatalogStore::list_columns(txn, table.id)?;
			let column_ids = pk_columns
				.iter()
				.map(|name| {
					table_columns.iter().find(|c| &c.name == name).map(|c| c.id).ok_or_else(|| {
						error!(internal!(
							"Primary key column '{}' not found in table '{}'",
							name,
							table.name
						))
					})
				})
				.collect::<crate::Result<Vec<_>>>()?;

			let _pk_id = CatalogStore::create_primary_key(
				txn,
				PrimaryKeyToCreate {
					primitive: PrimitiveId::Table(table.id),
					column_ids,
				},
			)?;

			// txn.track_primary_key_created(pk_id, PrimitiveId::Table(table.id))?;

			return Ok(CatalogStore::get_table(txn, table.id)?);
		}

		Ok(table)
	}

	#[instrument(name = "catalog::table::delete", level = "debug", skip(self, txn))]
	pub fn delete_table(&self, txn: &mut StandardCommandTransaction, table: TableDef) -> crate::Result<()> {
		CatalogStore::delete_table(txn, table.id)?;
		txn.track_table_def_deleted(table)?;
		Ok(())
	}

	/// Lists all tables in the catalog.
	#[instrument(name = "catalog::table::list_all", level = "debug", skip(self, txn))]
	pub fn list_tables_all<T: IntoStandardTransaction>(&self, txn: &mut T) -> crate::Result<Vec<TableDef>> {
		CatalogStore::list_tables_all(txn)
	}

	/// Lists all columns for a given table.
	#[instrument(name = "catalog::table::list_columns", level = "debug", skip(self, txn))]
	pub fn list_columns<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		table_id: TableId,
	) -> crate::Result<Vec<ColumnDef>> {
		CatalogStore::list_columns(txn, table_id)
	}

	/// Sets the primary key ID for a table.
	#[instrument(name = "catalog::table::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_table_primary_key(
		&self,
		txn: &mut StandardCommandTransaction,
		table_id: TableId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		CatalogStore::set_table_primary_key(txn, table_id, primary_key_id)
	}

	/// Gets the primary key ID for a table.
	#[instrument(name = "catalog::table::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_table_pk_id<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		CatalogStore::get_table_pk_id(txn, table_id)
	}
}
