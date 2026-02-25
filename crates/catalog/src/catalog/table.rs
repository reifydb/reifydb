// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::catalog::{
		change::CatalogTrackTableChangeOperations,
		column::{ColumnDef, ColumnIndex},
		id::{NamespaceId, PrimaryKeyId, TableId},
		policy::ColumnPolicyKind,
		primitive::PrimitiveId,
		table::TableDef,
	},
	internal,
	retention::RetentionPolicy,
};
use reifydb_transaction::{
	change::TransactionalTableChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{
	error,
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		primary_key::create::PrimaryKeyToCreate,
		table::create::{TableColumnToCreate as StoreTableColumnToCreate, TableToCreate as StoreTableToCreate},
	},
};

/// Column specification for table creation via Catalog API.
#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

/// Table creation specification for the Catalog API.
///
/// This struct includes `primary_key_columns` which allows specifying primary key
/// column names at creation time. The Catalog will handle resolving column names
/// to IDs and creating the primary key record.
#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub name: Fragment,
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
			fragment: col.fragment,
			constraint: col.constraint,
			policies: col.policies,
			auto_increment: col.auto_increment,
			dictionary_id: col.dictionary_id,
		}
	}
}

impl From<TableToCreate> for StoreTableToCreate {
	fn from(to_create: TableToCreate) -> Self {
		StoreTableToCreate {
			name: to_create.name,
			namespace: to_create.namespace,
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
			retention_policy: to_create.retention_policy,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::table::find", level = "trace", skip(self, txn))]
	pub fn find_table(&self, txn: &mut Transaction<'_>, id: TableId) -> crate::Result<Option<TableDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(table) = self.materialized.find_table_at(id, cmd.version()) {
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(table) = TransactionalTableChanges::find_table(admin, id) {
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted(admin, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) = self.materialized.find_table_at(id, admin.version()) {
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) = self.materialized.find_table_at(id, qry.version()) {
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table(&mut Transaction::Query(&mut *qry), id)? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::table::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_table_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<TableDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(table) =
					TransactionalTableChanges::find_table_by_name(admin, namespace, name)
				{
					return Ok(Some(table.clone()));
				}

				// 2. Check if deleted
				if TransactionalTableChanges::is_table_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(table));
				}

				// 4. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)? {
					warn!(
						"Table '{}' in namespace {:?} found in storage but not in MaterializedCatalog",
						name, namespace
					);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog (skip transactional changes)
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(table));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(table) = CatalogStore::find_table_by_name(
					&mut Transaction::Query(&mut *qry),
					namespace,
					name,
				)? {
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
	pub fn get_table(&self, txn: &mut Transaction<'_>, id: TableId) -> crate::Result<TableDef> {
		self.find_table(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::table::get_by_name", level = "trace", skip(self, txn, name))]
	pub fn get_table_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl Into<Fragment> + Send,
	) -> crate::Result<TableDef> {
		let name = name.into();

		// Try to get the namespace name for the error message
		let namespace_name = self
			.find_namespace(txn, namespace)?
			.map(|ns| ns.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		self.find_table_by_name(txn, namespace, name.text())?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Table,
				namespace: namespace_name,
				name: name.text().to_string(),
				fragment: name.clone(),
			}
			.into()
		})
	}

	#[instrument(name = "catalog::table::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_table(&self, txn: &mut AdminTransaction, to_create: TableToCreate) -> crate::Result<TableDef> {
		let pk_columns = to_create.primary_key_columns.clone();

		let table = CatalogStore::create_table(txn, to_create.into())?;
		txn.track_table_def_created(table.clone())?;

		let schema = Schema::from(table.columns.as_slice());
		let _registered_schema = self.schema.get_or_create(schema.fields().to_vec())?;

		if let Some(pk_columns) = pk_columns {
			let table_columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut *txn), table.id)?;
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

			return Ok(CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table.id)?);
		}

		Ok(table)
	}

	#[instrument(name = "catalog::table::drop", level = "debug", skip(self, txn))]
	pub fn drop_table(&self, txn: &mut AdminTransaction, table: TableDef) -> crate::Result<()> {
		CatalogStore::drop_table(txn, table.id)?;
		txn.track_table_def_deleted(table)?;
		Ok(())
	}

	/// Lists all tables in the catalog.
	#[instrument(name = "catalog::table::list_all", level = "debug", skip(self, txn))]
	pub fn list_tables_all(&self, txn: &mut Transaction<'_>) -> crate::Result<Vec<TableDef>> {
		CatalogStore::list_tables_all(txn)
	}

	/// Lists all columns for a given table.
	#[instrument(name = "catalog::table::list_columns", level = "debug", skip(self, txn))]
	pub fn list_columns(&self, txn: &mut Transaction<'_>, table_id: TableId) -> crate::Result<Vec<ColumnDef>> {
		CatalogStore::list_columns(txn, table_id)
	}

	/// Sets the primary key ID for a table.
	#[instrument(name = "catalog::table::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_table_primary_key(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		CatalogStore::set_table_primary_key(txn, table_id, primary_key_id)
	}

	/// Gets the primary key ID for a table.
	#[instrument(name = "catalog::table::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_table_pk_id(
		&self,
		txn: &mut Transaction<'_>,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		CatalogStore::get_table_pk_id(txn, table_id)
	}

	#[instrument(name = "catalog::table::add_column", level = "debug", skip(self, txn, column))]
	pub fn add_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		column: TableColumnToCreate,
		namespace_name: &str,
	) -> crate::Result<TableDef> {
		let pre = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
		let index = ColumnIndex(pre.columns.len() as u8);

		CatalogStore::create_column(
			txn,
			table_id,
			ColumnToCreate {
				fragment: Some(column.fragment.clone()),
				namespace_name: namespace_name.to_string(),
				primitive_name: pre.name.clone(),
				column: column.name.text().to_string(),
				constraint: column.constraint,
				policies: column.policies,
				index,
				auto_increment: column.auto_increment,
				dictionary_id: column.dictionary_id,
			},
		)?;

		let post = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
		txn.track_table_def_updated(pre, post.clone())?;

		Ok(post)
	}

	#[instrument(name = "catalog::table::drop_column", level = "debug", skip(self, txn))]
	pub fn drop_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		column_name: &str,
		namespace_name: &str,
	) -> crate::Result<TableDef> {
		let pre = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;

		let column = pre.columns.iter().find(|c| c.name == column_name).ok_or_else(|| {
			CatalogError::ColumnNotFound {
				kind: CatalogObjectKind::Table,
				namespace: namespace_name.to_string(),
				name: pre.name.clone(),
				column: column_name.to_string(),
				fragment: Fragment::None,
			}
		})?;

		CatalogStore::drop_column(txn, PrimitiveId::Table(table_id), column.id)?;

		let post = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
		txn.track_table_def_updated(pre, post.clone())?;

		Ok(post)
	}

	#[instrument(name = "catalog::table::rename_column", level = "debug", skip(self, txn))]
	pub fn rename_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		old_name: &str,
		new_name: &str,
		namespace_name: &str,
	) -> crate::Result<TableDef> {
		let pre = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;

		let column = pre.columns.iter().find(|c| c.name == old_name).ok_or_else(|| {
			CatalogError::ColumnNotFound {
				kind: CatalogObjectKind::Table,
				namespace: namespace_name.to_string(),
				name: pre.name.clone(),
				column: old_name.to_string(),
				fragment: Fragment::None,
			}
		})?;

		CatalogStore::rename_column(txn, PrimitiveId::Table(table_id), column.id, new_name)?;

		let post = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
		txn.track_table_def_updated(pre, post.clone())?;

		Ok(post)
	}
}
