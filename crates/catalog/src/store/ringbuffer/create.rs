// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::ringbuffer_already_exists,
	interface::catalog::{
		column::ColumnIndex,
		id::{NamespaceId, RingBufferId},
		policy::ColumnPolicyKind,
		ringbuffer::RingBufferDef,
	},
	key::{
		namespace_ringbuffer::NamespaceRingBufferKey,
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	fragment::Fragment,
	return_error,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};

use crate::{CatalogStore, store::sequence::system::SystemSequence};

#[derive(Debug, Clone)]
pub struct RingBufferColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct RingBufferToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

impl CatalogStore {
	pub(crate) fn create_ringbuffer(
		txn: &mut AdminTransaction,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		let namespace_id = to_create.namespace;

		if let Some(ringbuffer) = CatalogStore::find_ringbuffer_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return_error!(ringbuffer_already_exists(
				to_create.name.clone(),
				&namespace.name,
				&ringbuffer.name
			));
		}

		let ringbuffer_id = SystemSequence::next_ringbuffer_id(txn)?;

		Self::store_ringbuffer(txn, ringbuffer_id, namespace_id, &to_create)?;
		Self::link_ringbuffer_to_namespace(txn, namespace_id, ringbuffer_id, to_create.name.text())?;

		let capacity = to_create.capacity;

		Self::insert_ringbuffer_columns(txn, ringbuffer_id, to_create)?;
		Self::initialize_ringbuffer_metadata(txn, ringbuffer_id, capacity)?;

		Ok(Self::get_ringbuffer(&mut Transaction::Admin(&mut *txn), ringbuffer_id)?)
	}

	fn store_ringbuffer(
		txn: &mut AdminTransaction,
		ringbuffer: RingBufferId,
		namespace: NamespaceId,
		to_create: &RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::ringbuffer::schema::ringbuffer;

		let mut row = ringbuffer::SCHEMA.allocate();
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::ID, ringbuffer);
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::NAMESPACE, namespace);
		ringbuffer::SCHEMA.set_utf8(&mut row, ringbuffer::NAME, to_create.name.text());
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::CAPACITY, to_create.capacity);
		// Initialize with no primary key
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::PRIMARY_KEY, 0u64);

		txn.set(&RingBufferKey::encoded(ringbuffer), row)?;

		Ok(())
	}

	fn link_ringbuffer_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		ringbuffer: RingBufferId,
		name: &str,
	) -> crate::Result<()> {
		use crate::store::ringbuffer::schema::ringbuffer_namespace;

		let mut row = ringbuffer_namespace::SCHEMA.allocate();
		ringbuffer_namespace::SCHEMA.set_u64(&mut row, ringbuffer_namespace::ID, ringbuffer);
		ringbuffer_namespace::SCHEMA.set_utf8(&mut row, ringbuffer_namespace::NAME, name);

		txn.set(&NamespaceRingBufferKey::encoded(namespace, ringbuffer), row)?;

		Ok(())
	}

	fn insert_ringbuffer_columns(
		txn: &mut AdminTransaction,
		ringbuffer_id: RingBufferId,
		to_create: RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::column::create::ColumnToCreate;

		for (idx, col) in to_create.columns.into_iter().enumerate() {
			CatalogStore::create_column(
				txn,
				ringbuffer_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					primitive_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint,
					policies: col.policies,
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}

	fn initialize_ringbuffer_metadata(
		txn: &mut AdminTransaction,
		ringbuffer_id: RingBufferId,
		capacity: u64,
	) -> crate::Result<()> {
		use crate::store::ringbuffer::schema::ringbuffer_metadata;

		let mut row = ringbuffer_metadata::SCHEMA.allocate();
		ringbuffer_metadata::SCHEMA.set_u64(&mut row, ringbuffer_metadata::ID, ringbuffer_id);
		ringbuffer_metadata::SCHEMA.set_u64(&mut row, ringbuffer_metadata::CAPACITY, capacity);
		ringbuffer_metadata::SCHEMA.set_u64(&mut row, ringbuffer_metadata::HEAD, 0u64);
		ringbuffer_metadata::SCHEMA.set_u64(&mut row, ringbuffer_metadata::TAIL, 0u64);
		ringbuffer_metadata::SCHEMA.set_u64(&mut row, ringbuffer_metadata::COUNT, 0u64);

		txn.set(&RingBufferMetadataKey::encoded(ringbuffer_id), row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::key::namespace_ringbuffer::NamespaceRingBufferKey;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use super::*;
	use crate::{store::ringbuffer::schema::ringbuffer_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_simple_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("trades"),
			capacity: 1000,
			columns: vec![
				RingBufferColumnToCreate {
					name: Fragment::internal("symbol"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: Fragment::internal("price"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Float8),
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "trades");
		assert_eq!(result.capacity, 1000);
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "symbol");
		assert_eq!(result.columns[1].name, "price");
		assert_eq!(result.primary_key, None);
	}

	#[test]
	fn test_create_ringbuffer_empty_columns() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("empty_buffer"),
			capacity: 100,
			columns: vec![],
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "empty_buffer");
		assert_eq!(result.capacity, 100);
		assert_eq!(result.columns.len(), 0);
	}

	#[test]
	fn test_create_duplicate_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("test_ringbuffer"),
			capacity: 50,
			columns: vec![],
		};

		// First creation should succeed
		let result = CatalogStore::create_ringbuffer(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "test_ringbuffer");

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_ringbuffer_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("buffer1"),
			capacity: 10,
			columns: vec![],
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("buffer2"),
			capacity: 20,
			columns: vec![],
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Check namespace links
		let links: Vec<_> = txn
			.range(NamespaceRingBufferKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Check first link (descending order, so buffer2 comes first)
		let link = &links[0];
		let row = &link.values;
		let id2 = ringbuffer_namespace::SCHEMA.get_u64(row, ringbuffer_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(ringbuffer_namespace::SCHEMA.get_utf8(row, ringbuffer_namespace::NAME), "buffer2");

		// Check second link (buffer1 comes second)
		let link = &links[1];
		let row = &link.values;
		let id1 = ringbuffer_namespace::SCHEMA.get_u64(row, ringbuffer_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(ringbuffer_namespace::SCHEMA.get_utf8(row, ringbuffer_namespace::NAME), "buffer1");
	}

	#[test]
	fn test_create_ringbuffer_with_metadata() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("metadata_buffer"),
			capacity: 500,
			columns: vec![],
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Check that metadata was created
		let metadata = CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), result.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.id, result.id);
		assert_eq!(metadata.capacity, 500);
		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);
	}

	#[test]
	fn test_create_multiple_ringbuffers_with_different_capacities() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Create small buffer
		let small = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("small_buffer"),
			capacity: 10,
			columns: vec![],
		};
		let small_result = CatalogStore::create_ringbuffer(&mut txn, small).unwrap();
		assert_eq!(small_result.capacity, 10);

		// Create medium buffer
		let medium = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("medium_buffer"),
			capacity: 1000,
			columns: vec![],
		};
		let medium_result = CatalogStore::create_ringbuffer(&mut txn, medium).unwrap();
		assert_eq!(medium_result.capacity, 1000);

		// Create large buffer
		let large = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("large_buffer"),
			capacity: 1000000,
			columns: vec![],
		};
		let large_result = CatalogStore::create_ringbuffer(&mut txn, large).unwrap();
		assert_eq!(large_result.capacity, 1000000);

		// Verify they have different IDs
		assert_ne!(small_result.id, medium_result.id);
		assert_ne!(medium_result.id, large_result.id);
		assert_ne!(small_result.id, large_result.id);
	}

	#[test]
	fn test_create_ringbuffer_preserves_column_order() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let columns = vec![
			RingBufferColumnToCreate {
				fragment: Fragment::None,
				name: Fragment::internal("first"),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
			RingBufferColumnToCreate {
				fragment: Fragment::None,
				name: Fragment::internal("second"),
				constraint: TypeConstraint::unconstrained(Type::Uint16),
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
			RingBufferColumnToCreate {
				fragment: Fragment::None,
				name: Fragment::internal("third"),
				constraint: TypeConstraint::unconstrained(Type::Uint4),
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
		];

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("ordered_buffer"),
			capacity: 100,
			columns: columns.clone(),
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		assert_eq!(result.columns.len(), 3);
		assert_eq!(result.columns[0].name, "first");
		assert_eq!(result.columns[0].index.0, 0);
		assert_eq!(result.columns[1].name, "second");
		assert_eq!(result.columns[1].index.0, 1);
		assert_eq!(result.columns[2].name, "third");
		assert_eq!(result.columns[2].index.0, 2);
	}
}
