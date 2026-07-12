// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::SegmentTreeId,
	key::{
		namespace_segment_tree::NamespaceSegmentTreeKey,
		segment_tree::{SegmentTreeKey, SegmentTreeMetadataKey},
		segment_tree_node::SegmentTreeNodeKey,
	},
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};

use crate::{CatalogStore, Result, store::shape::drop::drop_shape_metadata};

impl CatalogStore {
	pub(crate) fn drop_segment_tree(txn: &mut AdminTransaction, segment_tree: SegmentTreeId) -> Result<()> {
		let pk_id = if let Some(segment_tree_def) =
			Self::find_segment_tree(&mut Transaction::Admin(&mut *txn), segment_tree)?
		{
			txn.remove(&NamespaceSegmentTreeKey::encoded(segment_tree_def.namespace, segment_tree))?;
			segment_tree_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		drop_shape_metadata(txn, segment_tree.into(), pk_id)?;

		Self::remove_segment_tree_nodes(txn, segment_tree)?;

		txn.remove(&SegmentTreeMetadataKey::encoded(segment_tree))?;

		txn.remove(&SegmentTreeKey::encoded(segment_tree))?;

		Ok(())
	}

	fn remove_segment_tree_nodes(txn: &mut AdminTransaction, segment_tree: SegmentTreeId) -> Result<()> {
		let range = SegmentTreeNodeKey::tree_prefix_range(segment_tree);
		let mut stream = txn.range(range, RangeScope::All, 1024)?;
		let mut keys = Vec::new();
		for entry in stream.by_ref() {
			keys.push(entry?.key.clone());
		}
		drop(stream);

		for key in keys {
			txn.remove(&key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::{
		interface::catalog::{id::SegmentTreeId, key::KeySpec},
		key::segment_tree_node::{SegmentTreeNodeKey, SegmentTreeScope},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{fragment::Fragment, util::cowvec::CowVec};

	use crate::{
		CatalogStore, store::segment_tree::create::SegmentTreeToCreate, test_utils::ensure_test_namespace,
	};

	fn key_spec(column: &str) -> KeySpec {
		KeySpec::Integer {
			column: column.to_string(),
		}
	}

	#[test]
	fn test_drop_segment_tree() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_segment_tree(
			&mut txn,
			SegmentTreeToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("drop_me"),
				key: key_spec("ts"),
				aggregates: vec![],
				columns: vec![],
				partition_by: vec![],
				underlying: false,
			},
		)
		.unwrap();

		let found = CatalogStore::find_segment_tree(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		CatalogStore::drop_segment_tree(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_segment_tree(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_segment_tree() {
		let mut txn = create_test_admin_transaction();

		let non_existent = SegmentTreeId(999999);
		let result = CatalogStore::drop_segment_tree(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_segment_tree_removes_node_entries() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_segment_tree(
			&mut txn,
			SegmentTreeToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("node_tree"),
				key: key_spec("ts"),
				aggregates: vec![],
				columns: vec![],
				partition_by: vec![],
				underlying: false,
			},
		)
		.unwrap();

		let node_keys = [
			SegmentTreeNodeKey::encoded(created.id, SegmentTreeScope::Global, 0, 0),
			SegmentTreeNodeKey::encoded(created.id, SegmentTreeScope::Global, 1, 3),
			SegmentTreeNodeKey::encoded(created.id, SegmentTreeScope::Global, 2, 7),
		];

		for key in &node_keys {
			txn.set(key, EncodedRow(CowVec::new(vec![1, 2, 3]))).unwrap();
		}

		for key in &node_keys {
			assert!(txn.get(key).unwrap().is_some());
		}

		CatalogStore::drop_segment_tree(&mut txn, created.id).unwrap();

		for key in &node_keys {
			assert!(txn.get(key).unwrap().is_none());
		}
	}
}
