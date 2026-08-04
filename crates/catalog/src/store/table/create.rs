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
	key::{namespace_table::NamespaceTableKey, table::TableKey},
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
	pub underlying: bool,
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
		let mut row = table::SHAPE.allocate();
		table::SHAPE.set::<u64>(&mut row, table::ID, u64::from(table));
		table::SHAPE.set::<u64>(&mut row, table::NAMESPACE, u64::from(namespace));
		table::SHAPE.set_utf8(&mut row, table::NAME, to_create.name.text());

		table::SHAPE.set::<u64>(&mut row, table::PRIMARY_KEY, 0u64);
		table::SHAPE.set_utf8(&mut row, table::PARTITION_BY, to_create.partition_by.join(","));
		table::SHAPE.set::<u8>(
			&mut row,
			table::UNDERLYING,
			if to_create.underlying {
				1
			} else {
				0
			},
		);
		write_time_source(&table::SHAPE, &mut row, table::TS, &to_create.time);

		txn.set(&TableKey::encoded(table), row.freeze())?;

		Ok(())
	}

	fn link_table_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		table: TableId,
		name: &str,
	) -> Result<()> {
		let mut row = table_namespace::SHAPE.allocate();
		table_namespace::SHAPE.set::<u64>(&mut row, table_namespace::ID, u64::from(table));
		table_namespace::SHAPE.set_utf8(&mut row, table_namespace::NAME, name);
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
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::id::{NamespaceId, TableId},
		key::namespace_table::NamespaceTableKey,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
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
			underlying: false,
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
			underlying: false,
			time: TimeSource::Processing,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("another_table"),
			columns: vec![],
			partition_by: vec![],
			underlying: false,
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
		let row = &link.row;
		assert_eq!(table_namespace::SHAPE.get::<u64>(row, table_namespace::ID), 16385);
		assert_eq!(table_namespace::SHAPE.get_utf8(row, table_namespace::NAME), "test_table");

		let link = &links[0];
		let row = &link.row;
		assert_eq!(table_namespace::SHAPE.get::<u64>(row, table_namespace::ID), 16386);
		assert_eq!(table_namespace::SHAPE.get_utf8(row, table_namespace::NAME), "another_table");
	}
}

#[cfg(test)]
mod time_declaration_tests {
	use reifydb_core::common::{TimeDomain, TimeSource};
	use reifydb_engine::test_harness::create_test_admin_transaction;
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
				underlying: false,
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
				underlying: false,
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(created.time, TimeSource::Processing);
		assert_eq!(created.time.ts(), None, "processing time must name no populator");
		assert_eq!(created.time.domain(), TimeDomain::Processing);
	}
}
