// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, TableId},
		property::ColumnPropertyKind,
		table::Table,
	},
	key::{catalog::TableKey, namespace::NamespaceTableKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		sequence::system::SystemSequence,
		table::shape::{table, table_namespace},
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
	pub partition_by: Vec<String>,
	pub time: TimeSource,
}

use crate::store::time_source::write_time_source;

impl CatalogStore {
	pub(crate) fn create_table(txn: &mut AdminTransaction, to_create: TableToCreate) -> Result<Table> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_table(txn, namespace_id, &to_create.name)?;

		let table_id = SystemSequence::next_table_id(txn)?;
		Self::store_table(txn, table_id, namespace_id, &to_create)?;
		Self::link_table_to_namespace(txn, namespace_id, table_id, to_create.name.text())?;

		Self::insert_columns(txn, table_id, to_create)?;

		Self::get_table(&mut Transaction::Admin(&mut *txn), table_id)
	}

	#[inline]
	fn reject_existing_table(txn: &mut AdminTransaction, namespace_id: NamespaceId, name: &Fragment) -> Result<()> {
		let Some(table) = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			name.text(),
		)?
		else {
			return Ok(());
		};
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::Table,
			namespace: namespace.name().to_string(),
			name: table.name,
			fragment: name.clone(),
		}
		.into())
	}

	fn store_table(
		txn: &mut AdminTransaction,
		table: TableId,
		namespace: NamespaceId,
		to_create: &TableToCreate,
	) -> Result<()> {
		let mut row = table::allocate();
		table::set_id(&mut row, u64::from(table));
		table::set_namespace(&mut row, u64::from(namespace));
		table::set_name(&mut row, to_create.name.text());

		table::set_primary_key(&mut row, 0u64);
		table::set_partition_by(&mut row, to_create.partition_by.join(","));
		write_time_source(&table::SHAPE, &mut row, table::TIME_DOMAIN, table::TS, &to_create.time);

		txn.set(&TableKey::encoded(table), row.freeze())?;

		Ok(())
	}

	fn link_table_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		table: TableId,
		name: &str,
	) -> Result<()> {
		let mut row = table_namespace::allocate();
		table_namespace::set_id(&mut row, u64::from(table));
		table_namespace::set_name(&mut row, name);
		txn.set(&NamespaceTableKey::encoded(namespace, table), row.freeze())?;
		Ok(())
	}

	fn insert_columns(txn: &mut AdminTransaction, table: TableId, to_create: TableToCreate) -> Result<()> {
		let namespace_name = Self::find_namespace(&mut Transaction::Admin(&mut *txn), to_create.namespace)?
			.map(|s| s.name().to_string())
			.unwrap_or_else(|| format!("namespace_{}", to_create.namespace));

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				table,
				ColumnToCreate {
					fragment: Some(column_to_create.fragment.clone()),
					namespace_name: namespace_name.clone(),
					object_name: to_create.name.text().to_string(),
					column: column_to_create.name.text().to_string(),
					constraint: column_to_create.constraint.clone(),
					properties: column_to_create.properties.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: column_to_create.auto_increment,
					dictionary_id: column_to_create.dictionary_id,
				},
			)?;
		}
		Ok(())
	}

	pub(crate) fn create_table_with_id(
		txn: &mut AdminTransaction,
		table_id: TableId,
		to_create: TableToCreate,
		column_ids: &[ColumnId],
	) -> Result<Table> {
		assert_eq!(column_ids.len(), to_create.columns.len(), "column_ids length must match columns length");

		let namespace_id = to_create.namespace;

		Self::store_table(txn, table_id, namespace_id, &to_create)?;
		Self::link_table_to_namespace(txn, namespace_id, table_id, to_create.name.text())?;

		Self::insert_columns_with_ids(txn, table_id, to_create, column_ids)?;

		Self::get_table(&mut Transaction::Admin(&mut *txn), table_id)
	}

	fn insert_columns_with_ids(
		txn: &mut AdminTransaction,
		table: TableId,
		to_create: TableToCreate,
		column_ids: &[ColumnId],
	) -> Result<()> {
		for (idx, (column_to_create, &col_id)) in
			to_create.columns.into_iter().zip(column_ids.iter()).enumerate()
		{
			Self::create_column_with_id(
				txn,
				col_id,
				table,
				ColumnToCreate {
					fragment: Some(column_to_create.fragment.clone()),
					namespace_name: String::new(),
					object_name: String::new(),
					column: column_to_create.name.text().to_string(),
					constraint: column_to_create.constraint.clone(),
					properties: column_to_create.properties.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: column_to_create.auto_increment,
					dictionary_id: column_to_create.dictionary_id,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::id::{NamespaceId, TableId},
		key::namespace::NamespaceTableKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::table::{create::TableToCreate, shape::table_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_admin_transaction();

		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("test_table"),
			columns: vec![],
			partition_by: vec![],
			time: TimeSource::Processing,
		};

		let result = CatalogStore::create_table(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, TableId(16385));
		assert_eq!(result.namespace, NamespaceId(16385));
		assert_eq!(result.name, "test_table");

		let err = CatalogStore::create_table(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_table_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("test_table"),
			columns: vec![],
			partition_by: vec![],
			time: TimeSource::Processing,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("another_table"),
			columns: vec![],
			partition_by: vec![],
			time: TimeSource::Processing,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceTableKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let bytes = &link.bytes;
		assert_eq!(table_namespace::get_id(EncodedCatalogRow::view(bytes)), 16385);
		assert_eq!(table_namespace::get_name(EncodedCatalogRow::view(bytes)), "test_table");

		let link = &links[0];
		let bytes = &link.bytes;
		assert_eq!(table_namespace::get_id(EncodedCatalogRow::view(bytes)), 16386);
		assert_eq!(table_namespace::get_name(EncodedCatalogRow::view(bytes)), "another_table");
	}
}

#[cfg(test)]
mod time_declaration_tests {
	use reifydb_core::common::{TimeDomain, TimeSource};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::fragment::Fragment;

	use super::*;
	use crate::{CatalogStore, test_utils::ensure_test_namespace};

	#[test]
	fn an_event_time_declaration_round_trips_through_the_catalog() {
		// The write boundary reads this declaration to populate #time on every row, so losing
		// it in the catalog silently downgrades an event-time table to processing time.
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				namespace: test_namespace.id(),
				name: Fragment::internal("trades"),
				columns: vec![],
				partition_by: vec![],
				time: TimeSource::Event {
					ts: "block_time".to_string(),
				},
			},
		)
		.unwrap();

		assert_eq!(created.time.ts(), Some("block_time"));

		let loaded = CatalogStore::find_table(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("table must be findable after creation");

		assert_eq!(
			loaded.time,
			TimeSource::Event {
				ts: "block_time".to_string()
			}
		);
	}
	#[test]
	fn a_processing_table_round_trips_with_no_populator() {
		// The domain is derived from the populator's presence and never stored beside it, so
		// no persisted object can claim event time while naming no column.
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				namespace: test_namespace.id(),
				name: Fragment::internal("audit"),
				columns: vec![],
				partition_by: vec![],
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(created.time, TimeSource::Processing);
		assert_eq!(created.time.ts(), None, "processing time must name no populator");
		assert_eq!(created.time.domain(), TimeDomain::Processing);
	}
}
