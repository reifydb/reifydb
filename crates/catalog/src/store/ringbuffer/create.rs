// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::ringbuffer_already_exists,
	interface::{
		ColumnIndex, ColumnPolicyKind, CommandTransaction, DictionaryId, NamespaceId, RingBufferDef,
		RingBufferId, TableId,
	},
	return_error,
};
use reifydb_type::{Fragment, TypeConstraint};

use crate::{CatalogStore, store::sequence::SystemSequence};

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
	pub async fn create_ringbuffer(
		txn: &mut impl CommandTransaction,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		let namespace_id = to_create.namespace;

		// Check if ring buffer already exists
		if let Some(ringbuffer) =
			CatalogStore::find_ringbuffer_by_name(txn, namespace_id, &to_create.ringbuffer).await?
		{
			let namespace = CatalogStore::get_namespace(txn, namespace_id).await?;
			return_error!(ringbuffer_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&ringbuffer.name
			));
		}

		// Allocate new ring buffer ID
		let ringbuffer_id = SystemSequence::next_ringbuffer_id(txn).await?;

		// Store the ring buffer
		Self::store_ringbuffer(txn, ringbuffer_id, namespace_id, &to_create).await?;

		// Link ring buffer to namespace
		Self::link_ringbuffer_to_namespace(txn, namespace_id, ringbuffer_id, &to_create.ringbuffer).await?;

		// Save capacity before moving to_create
		let capacity = to_create.capacity;

		// Insert columns
		Self::insert_ringbuffer_columns(txn, ringbuffer_id, to_create).await?;

		// Initialize ring buffer metadata
		Self::initialize_ringbuffer_metadata(txn, ringbuffer_id, capacity).await?;

		Ok(Self::get_ringbuffer(txn, ringbuffer_id).await?)
	}

	async fn store_ringbuffer(
		txn: &mut impl CommandTransaction,
		ringbuffer: RingBufferId,
		namespace: NamespaceId,
		to_create: &RingBufferToCreate,
	) -> crate::Result<()> {
		use reifydb_core::interface::RingBufferKey;

		use crate::store::ringbuffer::layout::ringbuffer;

		let mut row = ringbuffer::LAYOUT.allocate();
		ringbuffer::LAYOUT.set_u64(&mut row, ringbuffer::ID, ringbuffer);
		ringbuffer::LAYOUT.set_u64(&mut row, ringbuffer::NAMESPACE, namespace);
		ringbuffer::LAYOUT.set_utf8(&mut row, ringbuffer::NAME, &to_create.ringbuffer);
		ringbuffer::LAYOUT.set_u64(&mut row, ringbuffer::CAPACITY, to_create.capacity);
		// Initialize with no primary key
		ringbuffer::LAYOUT.set_u64(&mut row, ringbuffer::PRIMARY_KEY, 0u64);

		txn.set(&RingBufferKey::encoded(ringbuffer), row).await?;

		Ok(())
	}

	async fn link_ringbuffer_to_namespace(
		txn: &mut impl CommandTransaction,
		namespace: NamespaceId,
		ringbuffer: RingBufferId,
		name: &str,
	) -> crate::Result<()> {
		use reifydb_core::interface::NamespaceRingBufferKey;

		use crate::store::ringbuffer::layout::ringbuffer_namespace;

		let mut row = ringbuffer_namespace::LAYOUT.allocate();
		ringbuffer_namespace::LAYOUT.set_u64(&mut row, ringbuffer_namespace::ID, ringbuffer);
		ringbuffer_namespace::LAYOUT.set_utf8(&mut row, ringbuffer_namespace::NAME, name);

		txn.set(&NamespaceRingBufferKey::encoded(namespace, ringbuffer), row).await?;

		Ok(())
	}

	async fn insert_ringbuffer_columns(
		txn: &mut impl CommandTransaction,
		ringbuffer_id: RingBufferId,
		to_create: RingBufferToCreate,
	) -> crate::Result<()> {
		use crate::store::column::ColumnToCreate;

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
			)
			.await?;
		}

		Ok(())
	}

	async fn initialize_ringbuffer_metadata(
		txn: &mut impl CommandTransaction,
		ringbuffer_id: RingBufferId,
		capacity: u64,
	) -> crate::Result<()> {
		use reifydb_core::interface::RingBufferMetadataKey;

		use crate::store::ringbuffer::layout::ringbuffer_metadata;

		let mut row = ringbuffer_metadata::LAYOUT.allocate();
		ringbuffer_metadata::LAYOUT.set_u64(&mut row, ringbuffer_metadata::ID, ringbuffer_id);
		ringbuffer_metadata::LAYOUT.set_u64(&mut row, ringbuffer_metadata::CAPACITY, capacity);
		ringbuffer_metadata::LAYOUT.set_u64(&mut row, ringbuffer_metadata::HEAD, 0u64);
		ringbuffer_metadata::LAYOUT.set_u64(&mut row, ringbuffer_metadata::TAIL, 0u64);
		ringbuffer_metadata::LAYOUT.set_u64(&mut row, ringbuffer_metadata::COUNT, 0u64);

		txn.set(&RingBufferMetadataKey::encoded(ringbuffer_id), row).await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{NamespaceRingBufferKey, QueryTransaction};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use super::*;
	use crate::{store::ringbuffer::layout::ringbuffer_namespace, test_utils::ensure_test_namespace};

	#[tokio::test]
	async fn test_create_simple_ringbuffer() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

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

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "trades");
		assert_eq!(result.capacity, 1000);
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "symbol");
		assert_eq!(result.columns[1].name, "price");
		assert_eq!(result.primary_key, None);
	}

	#[tokio::test]
	async fn test_create_ringbuffer_empty_columns() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "empty_buffer".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "empty_buffer");
		assert_eq!(result.capacity, 100);
		assert_eq!(result.columns.len(), 0);
	}

	#[tokio::test]
	async fn test_create_duplicate_ringbuffer() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "test_ringbuffer".to_string(),
			capacity: 50,
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_ringbuffer(&mut txn, to_create.clone()).await.unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "test_ringbuffer");

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_005");
	}

	#[tokio::test]
	async fn test_ringbuffer_linked_to_namespace() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "buffer1".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "buffer2".to_string(),
			capacity: 20,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		// Check namespace links
		let links = txn
			.range(NamespaceRingBufferKey::full_scan(test_namespace.id))
			.await
			.unwrap()
			.items
			.into_iter()
			.collect::<Vec<_>>();
		assert_eq!(links.len(), 2);

		// Check first link (descending order, so buffer2 comes first)
		let link = &links[0];
		let row = &link.values;
		let id2 = ringbuffer_namespace::LAYOUT.get_u64(row, ringbuffer_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(ringbuffer_namespace::LAYOUT.get_utf8(row, ringbuffer_namespace::NAME), "buffer2");

		// Check second link (buffer1 comes second)
		let link = &links[1];
		let row = &link.values;
		let id1 = ringbuffer_namespace::LAYOUT.get_u64(row, ringbuffer_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(ringbuffer_namespace::LAYOUT.get_utf8(row, ringbuffer_namespace::NAME), "buffer1");
	}

	#[tokio::test]
	async fn test_create_ringbuffer_with_metadata() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "metadata_buffer".to_string(),
			capacity: 500,
			columns: vec![],
			fragment: None,
		};

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		// Check that metadata was created
		let metadata = CatalogStore::find_ringbuffer_metadata(&mut txn, result.id)
			.await
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.id, result.id);
		assert_eq!(metadata.capacity, 500);
		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);
	}

	#[tokio::test]
	async fn test_create_multiple_ringbuffers_with_different_capacities() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		// Create small buffer
		let small = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "small_buffer".to_string(),
			capacity: 10,
			columns: vec![],
			fragment: None,
		};
		let small_result = CatalogStore::create_ringbuffer(&mut txn, small).await.unwrap();
		assert_eq!(small_result.capacity, 10);

		// Create medium buffer
		let medium = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "medium_buffer".to_string(),
			capacity: 1000,
			columns: vec![],
			fragment: None,
		};
		let medium_result = CatalogStore::create_ringbuffer(&mut txn, medium).await.unwrap();
		assert_eq!(medium_result.capacity, 1000);

		// Create large buffer
		let large = RingBufferToCreate {
			namespace: test_namespace.id,
			ringbuffer: "large_buffer".to_string(),
			capacity: 1000000,
			columns: vec![],
			fragment: None,
		};
		let large_result = CatalogStore::create_ringbuffer(&mut txn, large).await.unwrap();
		assert_eq!(large_result.capacity, 1000000);

		// Verify they have different IDs
		assert_ne!(small_result.id, medium_result.id);
		assert_ne!(medium_result.id, large_result.id);
		assert_ne!(small_result.id, large_result.id);
	}

	#[tokio::test]
	async fn test_create_ringbuffer_preserves_column_order() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

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

		let result = CatalogStore::create_ringbuffer(&mut txn, to_create).await.unwrap();

		assert_eq!(result.columns.len(), 3);
		assert_eq!(result.columns[0].name, "first");
		assert_eq!(result.columns[0].index.0, 0);
		assert_eq!(result.columns[1].name, "second");
		assert_eq!(result.columns[1].index.0, 1);
		assert_eq!(result.columns[2].name, "third");
		assert_eq!(result.columns[2].index.0, 2);
	}
}
