// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::to_allocvec;
use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, SegmentTreeId},
		key::KeySpec,
		property::ColumnPropertyKind,
		segment_tree::{SegmentTree, SegmentTreeAggregate},
	},
	key::{
		namespace_segment_tree::NamespaceSegmentTreeKey,
		segment_tree::{SegmentTreeKey, SegmentTreeMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{blob::Blob, constraint::TypeConstraint, dictionary::DictionaryId},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		segment_tree::shape::{segment_tree, segment_tree_metadata, segment_tree_namespace},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct SegmentTreeColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct SegmentTreeToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<SegmentTreeColumnToCreate>,
	pub key: KeySpec,
	pub aggregates: Vec<SegmentTreeAggregate>,
	pub partition_by: Vec<String>,
	pub underlying: bool,
}

impl CatalogStore {
	pub(crate) fn create_segment_tree(
		txn: &mut AdminTransaction,
		to_create: SegmentTreeToCreate,
	) -> Result<SegmentTree> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_segment_tree(txn, namespace_id, &to_create.name)?;

		let segment_tree_id = SystemSequence::next_segment_tree_id(txn)?;
		Self::install_segment_tree(txn, segment_tree_id, namespace_id, &to_create)?;
		Self::insert_segment_tree_columns(txn, segment_tree_id, &to_create)?;
		Self::initialize_segment_tree_metadata(txn, segment_tree_id)?;
		Self::get_segment_tree(&mut Transaction::Admin(&mut *txn), segment_tree_id)
	}

	#[inline]
	fn reject_existing_segment_tree(
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		name: &Fragment,
	) -> Result<()> {
		let Some(segment_tree) = CatalogStore::find_segment_tree_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			name.text(),
		)?
		else {
			return Ok(());
		};
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::SegmentTree,
			namespace: namespace.name().to_string(),
			name: segment_tree.name,
			fragment: name.clone(),
		}
		.into())
	}

	#[inline]
	fn install_segment_tree(
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		namespace_id: NamespaceId,
		to_create: &SegmentTreeToCreate,
	) -> Result<()> {
		Self::store_segment_tree(txn, segment_tree_id, namespace_id, to_create)?;
		Self::link_segment_tree_to_namespace(txn, namespace_id, segment_tree_id, to_create.name.text())
	}

	fn store_segment_tree(
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		namespace: NamespaceId,
		to_create: &SegmentTreeToCreate,
	) -> Result<()> {
		let mut row = segment_tree::SHAPE.allocate();
		segment_tree::SHAPE.set_u64(&mut row, segment_tree::ID, segment_tree_id);
		segment_tree::SHAPE.set_u64(&mut row, segment_tree::NAMESPACE, namespace);
		segment_tree::SHAPE.set_utf8(&mut row, segment_tree::NAME, to_create.name.text());
		segment_tree::SHAPE.set_utf8(&mut row, segment_tree::KEY_COLUMN, to_create.key.column());
		let (key_kind_u8, precision_u8) = match &to_create.key {
			KeySpec::DateTime {
				precision,
				..
			} => (0u8, *precision as u8),
			KeySpec::Integer {
				..
			} => (1u8, 0u8),
		};
		segment_tree::SHAPE.set_u8(&mut row, segment_tree::KEY_KIND, key_kind_u8);
		segment_tree::SHAPE.set_u8(&mut row, segment_tree::PRECISION, precision_u8);
		segment_tree::SHAPE.set_u64(&mut row, segment_tree::PRIMARY_KEY, 0u64);
		segment_tree::SHAPE.set_utf8(&mut row, segment_tree::PARTITION_BY, to_create.partition_by.join(","));
		segment_tree::SHAPE.set_u8(
			&mut row,
			segment_tree::UNDERLYING,
			if to_create.underlying {
				1
			} else {
				0
			},
		);

		let aggregates_bytes = to_allocvec(&to_create.aggregates)
			.expect("SegmentTreeAggregate vec must serialize with postcard");
		segment_tree::SHAPE.set_blob(&mut row, segment_tree::AGGREGATES, &Blob::from(aggregates_bytes));

		txn.set(&SegmentTreeKey::encoded(segment_tree_id), row)?;

		Ok(())
	}

	fn link_segment_tree_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		segment_tree_id: SegmentTreeId,
		name: &str,
	) -> Result<()> {
		let mut row = segment_tree_namespace::SHAPE.allocate();
		segment_tree_namespace::SHAPE.set_u64(&mut row, segment_tree_namespace::ID, segment_tree_id);
		segment_tree_namespace::SHAPE.set_utf8(&mut row, segment_tree_namespace::NAME, name);

		txn.set(&NamespaceSegmentTreeKey::encoded(namespace, segment_tree_id), row)?;

		Ok(())
	}

	fn insert_segment_tree_columns(
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		to_create: &SegmentTreeToCreate,
	) -> Result<()> {
		for (idx, col) in to_create.columns.iter().enumerate() {
			CatalogStore::create_column(
				txn,
				segment_tree_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					shape_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint.clone(),
					properties: col.properties.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}

	fn initialize_segment_tree_metadata(txn: &mut AdminTransaction, segment_tree_id: SegmentTreeId) -> Result<()> {
		let mut row = segment_tree_metadata::SHAPE.allocate();
		segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::ID, segment_tree_id);
		segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::ROW_COUNT, 0u64);
		segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::OLDEST_KEY, 0u64);
		segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::NEWEST_KEY, 0u64);
		segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::SEQUENCE_COUNTER, 0u64);

		txn.set(&SegmentTreeMetadataKey::encoded(segment_tree_id), row)?;

		Ok(())
	}

	pub(crate) fn create_segment_tree_with_id(
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		to_create: SegmentTreeToCreate,
		column_ids: &[ColumnId],
	) -> Result<SegmentTree> {
		assert_eq!(column_ids.len(), to_create.columns.len(), "column_ids length must match columns length");

		let namespace_id = to_create.namespace;
		Self::install_segment_tree(txn, segment_tree_id, namespace_id, &to_create)?;
		Self::insert_segment_tree_columns_with_ids(txn, segment_tree_id, &to_create, column_ids)?;
		Self::initialize_segment_tree_metadata(txn, segment_tree_id)?;
		Self::get_segment_tree(&mut Transaction::Admin(&mut *txn), segment_tree_id)
	}

	fn insert_segment_tree_columns_with_ids(
		txn: &mut AdminTransaction,
		segment_tree_id: SegmentTreeId,
		to_create: &SegmentTreeToCreate,
		column_ids: &[ColumnId],
	) -> Result<()> {
		for (idx, (col, &col_id)) in to_create.columns.iter().zip(column_ids.iter()).enumerate() {
			CatalogStore::create_column_with_id(
				txn,
				col_id,
				segment_tree_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					shape_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint.clone(),
					properties: col.properties.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{key::KeySpec, segment_tree::SegmentTreeAggregate},
		key::namespace_segment_tree::NamespaceSegmentTreeKey,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use super::*;
	use crate::{store::segment_tree::shape::segment_tree_namespace, test_utils::ensure_test_namespace};

	fn key_spec(column: &str) -> KeySpec {
		KeySpec::Integer {
			column: column.to_string(),
		}
	}

	#[test]
	fn test_create_simple_segment_tree() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SegmentTreeToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("readings"),
			key: key_spec("ts"),
			aggregates: vec![SegmentTreeAggregate {
				name: "total".to_string(),
				monoid: "math::sum".to_string(),
				column: "load".to_string(),
			}],
			columns: vec![
				SegmentTreeColumnToCreate {
					name: Fragment::internal("ts"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				SegmentTreeColumnToCreate {
					name: Fragment::internal("load"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Float8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
			partition_by: vec![],
			underlying: false,
		};

		let result = CatalogStore::create_segment_tree(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id());
		assert_eq!(result.name, "readings");
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "ts");
		assert_eq!(result.columns[1].name, "load");
		assert_eq!(result.primary_key, None);
		assert_eq!(result.aggregates.len(), 1);
		assert_eq!(result.aggregates[0].name, "total");
		assert_eq!(result.aggregates[0].monoid, "math::sum");
		assert_eq!(result.aggregates[0].column, "load");
	}

	#[test]
	fn test_create_segment_tree_initializes_metadata() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SegmentTreeToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("metadata_tree"),
			key: key_spec("ts"),
			aggregates: vec![],
			columns: vec![],
			partition_by: vec![],
			underlying: false,
		};

		let result = CatalogStore::create_segment_tree(&mut txn, to_create).unwrap();

		let metadata = CatalogStore::find_segment_tree_metadata(&mut Transaction::Admin(&mut txn), result.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.id, result.id);
		assert_eq!(metadata.row_count, 0);
		assert_eq!(metadata.oldest_key, 0);
		assert_eq!(metadata.newest_key, 0);
		assert_eq!(metadata.sequence_counter, 0);
	}

	#[test]
	fn test_segment_tree_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SegmentTreeToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("tree1"),
			key: key_spec("ts"),
			aggregates: vec![],
			columns: vec![],
			partition_by: vec![],
			underlying: false,
		};

		let result = CatalogStore::create_segment_tree(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceSegmentTreeKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();
		assert_eq!(links.len(), 1);

		let row = &links[0].row;
		let id = segment_tree_namespace::SHAPE.get_u64(row, segment_tree_namespace::ID);
		assert_eq!(id, result.id.0);
		assert_eq!(segment_tree_namespace::SHAPE.get_utf8(row, segment_tree_namespace::NAME), "tree1");
	}

	#[test]
	fn test_create_duplicate_segment_tree() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SegmentTreeToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("dup_tree"),
			key: key_spec("ts"),
			aggregates: vec![],
			columns: vec![],
			partition_by: vec![],
			underlying: false,
		};

		CatalogStore::create_segment_tree(&mut txn, to_create.clone()).unwrap();

		let err = CatalogStore::create_segment_tree(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_segment_tree_aggregates_blob_roundtrip() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let aggregates = vec![
			SegmentTreeAggregate {
				name: "total".to_string(),
				monoid: "math::sum".to_string(),
				column: "load".to_string(),
			},
			SegmentTreeAggregate {
				name: "peak".to_string(),
				monoid: "math::max".to_string(),
				column: "load".to_string(),
			},
		];

		let to_create = SegmentTreeToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("agg_tree"),
			key: key_spec("ts"),
			aggregates: aggregates.clone(),
			columns: vec![],
			partition_by: vec![],
			underlying: false,
		};

		let created = CatalogStore::create_segment_tree(&mut txn, to_create).unwrap();
		assert_eq!(created.aggregates, aggregates);

		let read_back = CatalogStore::get_segment_tree(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert_eq!(read_back.aggregates, aggregates);
	}
}
