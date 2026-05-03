// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::shape::RowShape,
	interface::catalog::{
		change::CatalogTrackTableChangeOperations,
		column::{Column, ColumnIndex},
		id::{ColumnId, NamespaceId, PrimaryKeyId, TableId},
		property::ColumnPropertyKind,
		shape::ShapeId,
		table::Table,
	},
	internal,
	retention::RetentionStrategy,
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
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		primary_key::create::PrimaryKeyToCreate,
		table::create::{TableColumnToCreate as StoreTableColumnToCreate, TableToCreate as StoreTableToCreate},
	},
};

#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<TableColumnToCreate>,
	pub retention_strategy: Option<RetentionStrategy>,

	pub primary_key_columns: Option<Vec<String>>,
	pub underlying: bool,
}

impl From<TableColumnToCreate> for StoreTableColumnToCreate {
	fn from(col: TableColumnToCreate) -> Self {
		StoreTableColumnToCreate {
			name: col.name,
			fragment: col.fragment,
			constraint: col.constraint,
			properties: col.properties,
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
			retention_strategy: to_create.retention_strategy,
			underlying: to_create.underlying,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::table::find", level = "trace", skip(self, txn))]
	pub fn find_table(&self, txn: &mut Transaction<'_>, id: TableId) -> Result<Option<Table>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(table) = self.materialized.find_table_at(id, cmd.version()) {
					return Ok(Some(table));
				}

				if let Some(table) = CatalogStore::find_table(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(table) = TransactionalTableChanges::find_table(admin, id) {
					return Ok(Some(table.clone()));
				}

				if TransactionalTableChanges::is_table_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(table) = self.materialized.find_table_at(id, admin.version()) {
					return Ok(Some(table));
				}

				if let Some(table) = CatalogStore::find_table(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(table) = self.materialized.find_table_at(id, qry.version()) {
					return Ok(Some(table));
				}

				if let Some(table) = CatalogStore::find_table(&mut Transaction::Query(&mut *qry), id)? {
					warn!("Table with ID {:?} found in storage but not in MaterializedCatalog", id);
					return Ok(Some(table));
				}

				Ok(None)
			}
			Transaction::Test(mut t) => {
				if let Some(table) = TransactionalTableChanges::find_table(t.inner, id) {
					return Ok(Some(table.clone()));
				}
				if TransactionalTableChanges::is_table_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(table) =
					CatalogStore::find_table(&mut Transaction::Test(Box::new(t.reborrow())), id)?
				{
					return Ok(Some(table));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(table) = self.materialized.find_table_at(id, rep.version()) {
					return Ok(Some(table));
				}

				if let Some(table) = CatalogStore::find_table(&mut Transaction::Replica(&mut *rep), id)?
				{
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
	) -> Result<Option<Table>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(table));
				}

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
				if let Some(table) =
					TransactionalTableChanges::find_table_by_name(admin, namespace, name)
				{
					return Ok(Some(table.clone()));
				}

				if TransactionalTableChanges::is_table_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(table));
				}

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
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(table));
				}

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
			Transaction::Test(mut t) => {
				if let Some(table) =
					TransactionalTableChanges::find_table_by_name(t.inner, namespace, name)
				{
					return Ok(Some(table.clone()));
				}
				if TransactionalTableChanges::is_table_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(table) = CatalogStore::find_table_by_name(
					&mut Transaction::Test(Box::new(t.reborrow())),
					namespace,
					name,
				)? {
					return Ok(Some(table));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(table) =
					self.materialized.find_table_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(table));
				}

				if let Some(table) = CatalogStore::find_table_by_name(
					&mut Transaction::Replica(&mut *rep),
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
	pub fn get_table(&self, txn: &mut Transaction<'_>, id: TableId) -> Result<Table> {
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
	) -> Result<Table> {
		let name = name.into();

		let namespace_name = self
			.find_namespace(txn, namespace)?
			.map(|ns| ns.name().to_string())
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
	pub fn create_table(&self, txn: &mut AdminTransaction, to_create: TableToCreate) -> Result<Table> {
		let pk_columns = to_create.primary_key_columns.clone();
		let table = CatalogStore::create_table(txn, to_create.into())?;
		self.finalize_created_table(txn, table, pk_columns)
	}

	pub fn create_table_with_id(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		to_create: TableToCreate,
		column_ids: &[ColumnId],
	) -> Result<Table> {
		let pk_columns = to_create.primary_key_columns.clone();
		let table = CatalogStore::create_table_with_id(txn, table_id, to_create.into(), column_ids)?;
		self.finalize_created_table(txn, table, pk_columns)
	}

	#[inline]
	fn finalize_created_table(
		&self,
		txn: &mut AdminTransaction,
		table: Table,
		pk_columns: Option<Vec<String>>,
	) -> Result<Table> {
		txn.track_table_created(table.clone())?;
		let shape = RowShape::from(table.columns.as_slice());
		self.get_or_create_row_shape(&mut Transaction::Admin(&mut *txn), shape.fields().to_vec())?;

		let Some(pk_columns) = pk_columns else {
			return Ok(table);
		};
		let column_ids = resolve_pk_column_ids(&mut Transaction::Admin(&mut *txn), &table, &pk_columns)?;
		CatalogStore::create_primary_key(
			txn,
			PrimaryKeyToCreate {
				shape: ShapeId::Table(table.id),
				column_ids,
			},
		)?;
		CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table.id)
	}

	#[instrument(name = "catalog::table::drop", level = "debug", skip(self, txn))]
	pub fn drop_table(&self, txn: &mut AdminTransaction, table: Table) -> Result<()> {
		CatalogStore::drop_table(txn, table.id)?;
		txn.track_table_deleted(table)?;
		Ok(())
	}

	#[instrument(name = "catalog::table::list_all", level = "debug", skip(self, txn))]
	pub fn list_tables_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Table>> {
		CatalogStore::list_tables_all(txn)
	}

	#[instrument(name = "catalog::table::list_columns", level = "debug", skip(self, txn))]
	pub fn list_columns(&self, txn: &mut Transaction<'_>, table_id: TableId) -> Result<Vec<Column>> {
		CatalogStore::list_columns(txn, table_id)
	}

	#[instrument(name = "catalog::table::set_primary_key", level = "debug", skip(self, txn))]
	pub fn set_table_primary_key(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		primary_key_id: PrimaryKeyId,
	) -> Result<()> {
		CatalogStore::set_table_primary_key(txn, table_id, primary_key_id)
	}

	#[instrument(name = "catalog::table::get_pk_id", level = "trace", skip(self, txn))]
	pub fn get_table_pk_id(&self, txn: &mut Transaction<'_>, table_id: TableId) -> Result<Option<PrimaryKeyId>> {
		CatalogStore::get_table_pk_id(txn, table_id)
	}

	#[instrument(name = "catalog::table::add_column", level = "debug", skip(self, txn, column))]
	pub fn add_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		column: TableColumnToCreate,
		namespace_name: &str,
	) -> Result<Table> {
		alter_table_with_tracking(txn, table_id, |txn, pre| {
			let index = ColumnIndex(pre.columns.len() as u8);
			CatalogStore::create_column(
				txn,
				table_id,
				ColumnToCreate {
					fragment: Some(column.fragment.clone()),
					namespace_name: namespace_name.to_string(),
					shape_name: pre.name.clone(),
					column: column.name.text().to_string(),
					constraint: column.constraint,
					properties: column.properties,
					index,
					auto_increment: column.auto_increment,
					dictionary_id: column.dictionary_id,
				},
			)?;
			Ok(())
		})
	}

	#[instrument(name = "catalog::table::drop_column", level = "debug", skip(self, txn))]
	pub fn drop_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		column_name: &str,
		namespace_name: &str,
	) -> Result<Table> {
		alter_table_with_tracking(txn, table_id, |txn, pre| {
			let column = find_column_or_error(pre, column_name, namespace_name)?;
			CatalogStore::drop_column(txn, ShapeId::Table(table_id), column.id)?;
			Ok(())
		})
	}

	#[instrument(name = "catalog::table::rename_column", level = "debug", skip(self, txn))]
	pub fn rename_table_column(
		&self,
		txn: &mut AdminTransaction,
		table_id: TableId,
		old_name: &str,
		new_name: &str,
		namespace_name: &str,
	) -> Result<Table> {
		alter_table_with_tracking(txn, table_id, |txn, pre| {
			let column = find_column_or_error(pre, old_name, namespace_name)?;
			CatalogStore::rename_column(txn, ShapeId::Table(table_id), column.id, new_name)?;
			Ok(())
		})
	}
}

#[inline]
fn alter_table_with_tracking<F>(txn: &mut AdminTransaction, table_id: TableId, mutate: F) -> Result<Table>
where
	F: FnOnce(&mut AdminTransaction, &Table) -> Result<()>,
{
	let pre = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
	mutate(txn, &pre)?;
	let post = CatalogStore::get_table(&mut Transaction::Admin(&mut *txn), table_id)?;
	txn.track_table_updated(pre, post.clone())?;
	Ok(post)
}

#[inline]
fn find_column_or_error<'a>(table: &'a Table, column_name: &str, namespace_name: &str) -> Result<&'a Column> {
	table.columns.iter().find(|c| c.name == column_name).ok_or_else(|| {
		CatalogError::ColumnNotFound {
			kind: CatalogObjectKind::Table,
			namespace: namespace_name.to_string(),
			name: table.name.clone(),
			column: column_name.to_string(),
			fragment: Fragment::None,
		}
		.into()
	})
}

#[inline]
fn resolve_pk_column_ids(
	txn: &mut Transaction<'_>,
	table: &Table,
	pk_column_names: &[String],
) -> Result<Vec<ColumnId>> {
	let table_columns = CatalogStore::list_columns(txn, table.id)?;
	pk_column_names
		.iter()
		.map(|name| {
			table_columns.iter().find(|c| &c.name == name).map(|c| c.id).ok_or_else(|| {
				error!(internal!("Primary key column '{}' not found in table '{}'", name, table.name))
			})
		})
		.collect()
}
