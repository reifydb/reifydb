// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::ring_buffer_already_exists,
	interface::{
		ColumnIndex, ColumnPolicyKind, CommandTransaction, NamespaceId, RingBufferDef, RingBufferId, TableId,
	},
	return_error,
};
use reifydb_type::{OwnedFragment, TypeConstraint};

use crate::{CatalogStore, store::sequence::SystemSequence};

#[derive(Debug, Clone)]
pub struct RingBufferColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct RingBufferToCreate {
	pub fragment: Option<OwnedFragment>,
	pub ring_buffer: String,
	pub namespace: NamespaceId,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

impl CatalogStore {
	pub fn create_ring_buffer(
		txn: &mut impl CommandTransaction,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		let namespace_id = to_create.namespace;

		// Check if ring buffer already exists
		if let Some(ring_buffer) =
			CatalogStore::find_ring_buffer_by_name(txn, namespace_id, &to_create.ring_buffer)?
		{
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(ring_buffer_already_exists(
				to_create.fragment,
				&namespace.name,
				&ring_buffer.name
			));
		}

		// Allocate new ring buffer ID
		let ring_buffer_id = SystemSequence::next_ring_buffer_id(txn)?;

		// Store the ring buffer
		Self::store_ring_buffer(txn, ring_buffer_id, namespace_id, &to_create)?;

		// Link ring buffer to namespace
		Self::link_ring_buffer_to_namespace(txn, namespace_id, ring_buffer_id, &to_create.ring_buffer)?;

		// Save capacity before moving to_create
		let capacity = to_create.capacity;

		// Insert columns
		Self::insert_ring_buffer_columns(txn, ring_buffer_id, to_create)?;

		// Initialize ring buffer metadata
		Self::initialize_ring_buffer_metadata(txn, ring_buffer_id, capacity)?;

		Ok(Self::get_ring_buffer(txn, ring_buffer_id)?)
	}

	fn store_ring_buffer(
		txn: &mut impl CommandTransaction,
		ring_buffer: RingBufferId,
		namespace: NamespaceId,
		to_create: &RingBufferToCreate,
	) -> crate::Result<()> {
		use reifydb_core::interface::{EncodableKey, RingBufferKey};

		use crate::store::ring_buffer::layout::ring_buffer;

		let mut row = ring_buffer::LAYOUT.allocate();
		ring_buffer::LAYOUT.set_u64(&mut row, ring_buffer::ID, ring_buffer);
		ring_buffer::LAYOUT.set_u64(&mut row, ring_buffer::NAMESPACE, namespace);
		ring_buffer::LAYOUT.set_utf8(&mut row, ring_buffer::NAME, &to_create.ring_buffer);
		ring_buffer::LAYOUT.set_u64(&mut row, ring_buffer::CAPACITY, to_create.capacity);
		// Initialize with no primary key
		ring_buffer::LAYOUT.set_u64(&mut row, ring_buffer::PRIMARY_KEY, 0u64);

		let key = RingBufferKey::new(ring_buffer);
		txn.set(&key.encode(), row)?;

		Ok(())
	}

	fn link_ring_buffer_to_namespace(
		txn: &mut impl CommandTransaction,
		namespace: NamespaceId,
		ring_buffer: RingBufferId,
		name: &str,
	) -> crate::Result<()> {
		use reifydb_core::interface::{EncodableKey, NamespaceRingBufferKey};

		use crate::store::ring_buffer::layout::ring_buffer_namespace;

		let mut row = ring_buffer_namespace::LAYOUT.allocate();
		ring_buffer_namespace::LAYOUT.set_u64(&mut row, ring_buffer_namespace::ID, ring_buffer);
		ring_buffer_namespace::LAYOUT.set_utf8(&mut row, ring_buffer_namespace::NAME, name);

		let key = NamespaceRingBufferKey::new(namespace, ring_buffer);
		txn.set(&key.encode(), row)?;

		Ok(())
	}

	fn insert_ring_buffer_columns(
		txn: &mut impl CommandTransaction,
		ring_buffer_id: RingBufferId,
		to_create: RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::column::ColumnToCreate;

		for (idx, col) in to_create.columns.into_iter().enumerate() {
			CatalogStore::create_column(
				txn,
				ring_buffer_id,
				ColumnToCreate {
					fragment: col.fragment,
					namespace_name: "", /* Not used in
					                     * create_column */
					table: TableId(0), /* Not used in
					                    * create_column -
					                    * source is passed
					                    * separately */
					table_name: "", /* Not used in
					                 * create_column */
					column: col.name,
					constraint: col.constraint,
					if_not_exists: false,
					policies: col.policies,
					index: ColumnIndex(idx as u16),
					auto_increment: col.auto_increment,
				},
			)?;
		}

		Ok(())
	}

	fn initialize_ring_buffer_metadata(
		txn: &mut impl CommandTransaction,
		ring_buffer_id: RingBufferId,
		capacity: u64,
	) -> crate::Result<()> {
		use reifydb_core::interface::{EncodableKey, RingBufferMetadataKey};

		use crate::store::ring_buffer::layout::ring_buffer_metadata;

		let mut row = ring_buffer_metadata::LAYOUT.allocate();
		ring_buffer_metadata::LAYOUT.set_u64(&mut row, ring_buffer_metadata::ID, ring_buffer_id);
		ring_buffer_metadata::LAYOUT.set_u64(&mut row, ring_buffer_metadata::CAPACITY, capacity);
		ring_buffer_metadata::LAYOUT.set_u64(&mut row, ring_buffer_metadata::HEAD, 0u64);
		ring_buffer_metadata::LAYOUT.set_u64(&mut row, ring_buffer_metadata::TAIL, 0u64);
		ring_buffer_metadata::LAYOUT.set_u64(&mut row, ring_buffer_metadata::COUNT, 0u64);

		let key = RingBufferMetadataKey::new(ring_buffer_id);
		txn.set(&key.encode(), row)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{MultiVersionQueryTransaction, NamespaceRingBufferKey};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use super::*;
	use crate::{store::ring_buffer::layout::ring_buffer_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_simple_ring_buffer() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "trades".to_string(),
			capacity: 1000,
			columns: vec![
				RingBufferColumnToCreate {
					name: "symbol".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
				},
				RingBufferColumnToCreate {
					name: "price".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Float8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
				},
			],
			fragment: None,
		};

		let result = CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

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
	fn test_create_ring_buffer_empty_columns() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "empty_buffer".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};

		let result = CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "empty_buffer");
		assert_eq!(result.capacity, 100);
		assert_eq!(result.columns.len(), 0);
	}

	#[test]
	fn test_create_duplicate_ring_buffer() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "test_ring_buffer".to_string(),
			capacity: 50,
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_ring_buffer(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "test_ring_buffer");

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[test]
	fn test_ring_buffer_linked_to_namespace() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "buffer1".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "buffer2".to_string(),
			capacity: 20,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		// Check namespace links
		let links =
			txn.range(NamespaceRingBufferKey::full_scan(test_namespace.id)).unwrap().collect::<Vec<_>>();
		assert_eq!(links.len(), 2);

		// Check first link (descending order, so buffer2 comes first)
		let link = &links[0];
		let row = &link.values;
		let id2 = ring_buffer_namespace::LAYOUT.get_u64(row, ring_buffer_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(ring_buffer_namespace::LAYOUT.get_utf8(row, ring_buffer_namespace::NAME), "buffer2");

		// Check second link (buffer1 comes second)
		let link = &links[1];
		let row = &link.values;
		let id1 = ring_buffer_namespace::LAYOUT.get_u64(row, ring_buffer_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(ring_buffer_namespace::LAYOUT.get_utf8(row, ring_buffer_namespace::NAME), "buffer1");
	}

	#[test]
	fn test_create_ring_buffer_with_metadata() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "metadata_buffer".to_string(),
			capacity: 500,
			columns: vec![],
			fragment: None,
		};

		let result = CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		// Check that metadata was created
		let metadata = CatalogStore::find_ring_buffer_metadata(&mut txn, result.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.id, result.id);
		assert_eq!(metadata.capacity, 500);
		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);
	}

	#[test]
	fn test_create_multiple_ring_buffers_with_different_capacities() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Create small buffer
		let small = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "small_buffer".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};
		let small_result = CatalogStore::create_ring_buffer(&mut txn, small).unwrap();
		assert_eq!(small_result.capacity, 10);

		// Create medium buffer
		let medium = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "medium_buffer".to_string(),
			capacity: 1000,
			columns: vec![],
			fragment: None,
		};
		let medium_result = CatalogStore::create_ring_buffer(&mut txn, medium).unwrap();
		assert_eq!(medium_result.capacity, 1000);

		// Create large buffer
		let large = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "large_buffer".to_string(),
			capacity: 1000000,
			columns: vec![],
			fragment: None,
		};
		let large_result = CatalogStore::create_ring_buffer(&mut txn, large).unwrap();
		assert_eq!(large_result.capacity, 1000000);

		// Verify they have different IDs
		assert_ne!(small_result.id, medium_result.id);
		assert_ne!(medium_result.id, large_result.id);
		assert_ne!(small_result.id, large_result.id);
	}

	#[test]
	fn test_create_ring_buffer_preserves_column_order() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let columns = vec![
			RingBufferColumnToCreate {
				name: "first".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				fragment: None,
				policies: vec![],
				auto_increment: false,
			},
			RingBufferColumnToCreate {
				name: "second".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint16),
				fragment: None,
				policies: vec![],
				auto_increment: false,
			},
			RingBufferColumnToCreate {
				name: "third".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint4),
				fragment: None,
				policies: vec![],
				auto_increment: false,
			},
		];

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ring_buffer: "ordered_buffer".to_string(),
			capacity: 100,
			columns: columns.clone(),
			fragment: None,
		};

		let result = CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		assert_eq!(result.columns.len(), 3);
		assert_eq!(result.columns[0].name, "first");
		assert_eq!(result.columns[0].index.0, 0);
		assert_eq!(result.columns[1].name, "second");
		assert_eq!(result.columns[1].index.0, 1);
		assert_eq!(result.columns[2].name, "third");
		assert_eq!(result.columns[2].index.0, 2);
	}
}
