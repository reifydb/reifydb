// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{DictionaryId, NamespaceId, RingBufferId, TableId},
		policy::ColumnPolicyKind,
		ringbuffer::RingBufferDef,
	},
	key::{
		namespace_ringbuffer::NamespaceRingBufferKey,
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
	},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::{
	error::diagnostic::catalog::ringbuffer_already_exists, fragment::Fragment, return_error,
	value::constraint::TypeConstraint,
};

use crate::{CatalogStore, store::sequence::system::SystemSequence};

#[derive(Debug, Clone)]
pub struct RingBufferColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<Fragment>,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct RingBufferToCreate {
	pub fragment: Option<Fragment>,
	pub ringbuffer: String,
	pub namespace: NamespaceId,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

impl CatalogStore {
	pub fn create_ringbuffer(
		txn: &mut StandardCommandTransaction,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		let namespace_id = to_create.namespace;

		if let Some(ringbuffer) =
			CatalogStore::find_ringbuffer_by_name(txn, namespace_id, &to_create.ringbuffer)?
		{
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(ringbuffer_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&ringbuffer.name
			));
		}

		let ringbuffer_id = SystemSequence::next_ringbuffer_id(txn)?;

		Self::store_ringbuffer(txn, ringbuffer_id, namespace_id, &to_create)?;
		Self::link_ringbuffer_to_namespace(txn, namespace_id, ringbuffer_id, &to_create.ringbuffer)?;

		let capacity = to_create.capacity;

		Self::insert_ringbuffer_columns(txn, ringbuffer_id, to_create)?;
		Self::initialize_ringbuffer_metadata(txn, ringbuffer_id, capacity)?;

		Ok(Self::get_ringbuffer(txn, ringbuffer_id)?)
	}

	fn store_ringbuffer(
		txn: &mut StandardCommandTransaction,
		ringbuffer: RingBufferId,
		namespace: NamespaceId,
		to_create: &RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::ringbuffer::schema::ringbuffer;

		let mut row = ringbuffer::SCHEMA.allocate();
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::ID, ringbuffer);
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::NAMESPACE, namespace);
		ringbuffer::SCHEMA.set_utf8(&mut row, ringbuffer::NAME, &to_create.ringbuffer);
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::CAPACITY, to_create.capacity);
		// Initialize with no primary key
		ringbuffer::SCHEMA.set_u64(&mut row, ringbuffer::PRIMARY_KEY, 0u64);

		txn.set(&RingBufferKey::encoded(ringbuffer), row)?;

		Ok(())
	}

	fn link_ringbuffer_to_namespace(
		txn: &mut StandardCommandTransaction,
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
		txn: &mut StandardCommandTransaction,
		ringbuffer_id: RingBufferId,
		to_create: RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::column::create::ColumnToCreate;

		for (idx, col) in to_create.columns.into_iter().enumerate() {
			CatalogStore::create_column(
				txn,
				ringbuffer_id,
				ColumnToCreate {
					fragment: col.fragment,
					namespace_name: String::new(), /* Not used in
					                                * create_column */
					table: TableId(0), /* Not used in
					                    * create_column -
					                    * source is passed
					                    * separately */
					table_name: String::new(), /* Not used in
					                            * create_column */
					column: col.name,
					constraint: col.constraint,
					if_not_exists: false,
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
		txn: &mut StandardCommandTransaction,
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
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use super::*;
	use crate::{store::ringbuffer::schema::ringbuffer_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_simple_ringbuffer() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "trades".to_string(),
			capacity: 1000,
			columns: vec![
				RingBufferColumnToCreate {
					name: "symbol".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: "price".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Float8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
			fragment: None,
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "empty_buffer".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "test_ringbuffer".to_string(),
			capacity: 50,
			columns: vec![],
			fragment: None,
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "buffer1".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "buffer2".to_string(),
			capacity: 20,
			columns: vec![],
			fragment: None,
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "metadata_buffer".to_string(),
			capacity: 500,
			columns: vec![],
			fragment: None,
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Check that metadata was created
		let metadata = CatalogStore::find_ringbuffer_metadata(&mut txn, result.id)
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Create small buffer
		let small = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "small_buffer".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};
		let small_result = CatalogStore::create_ringbuffer(&mut txn, small).unwrap();
		assert_eq!(small_result.capacity, 10);

		// Create medium buffer
		let medium = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "medium_buffer".to_string(),
			capacity: 1000,
			columns: vec![],
			fragment: None,
		};
		let medium_result = CatalogStore::create_ringbuffer(&mut txn, medium).unwrap();
		assert_eq!(medium_result.capacity, 1000);

		// Create large buffer
		let large = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "large_buffer".to_string(),
			capacity: 1000000,
			columns: vec![],
			fragment: None,
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
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let columns = vec![
			RingBufferColumnToCreate {
				name: "first".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				fragment: None,
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
			RingBufferColumnToCreate {
				name: "second".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint16),
				fragment: None,
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
			RingBufferColumnToCreate {
				name: "third".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint4),
				fragment: None,
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
		];

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "ordered_buffer".to_string(),
			capacity: 100,
			columns: columns.clone(),
			fragment: None,
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
