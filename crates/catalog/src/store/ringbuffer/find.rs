// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, RingBufferId},
		ringbuffer::{RingBufferDef, RingBufferMetadata},
	},
	key::{
		namespace_ringbuffer::NamespaceRingBufferKey,
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
	},
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	CatalogStore,
	store::ringbuffer::schema::{ringbuffer, ringbuffer_metadata, ringbuffer_namespace},
};

impl CatalogStore {
	pub(crate) fn find_ringbuffer(
		rx: &mut impl AsTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&RingBufferKey::encoded(ringbuffer))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = RingBufferId(ringbuffer::SCHEMA.get_u64(&row, ringbuffer::ID));
		let namespace = NamespaceId(ringbuffer::SCHEMA.get_u64(&row, ringbuffer::NAMESPACE));
		let name = ringbuffer::SCHEMA.get_utf8(&row, ringbuffer::NAME).to_string();
		let capacity = ringbuffer::SCHEMA.get_u64(&row, ringbuffer::CAPACITY);

		Ok(Some(RingBufferDef {
			id,
			namespace,
			name,
			capacity,
			columns: Self::list_columns(&mut txn, id)?,
			primary_key: Self::find_primary_key(&mut txn, id)?,
		}))
	}

	pub(crate) fn find_ringbuffer_metadata(
		rx: &mut impl AsTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<Option<RingBufferMetadata>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&RingBufferMetadataKey::encoded(ringbuffer))? else {
			return Ok(None);
		};

		let row = multi.values;
		let buffer_id = RingBufferId(ringbuffer_metadata::SCHEMA.get_u64(&row, ringbuffer_metadata::ID));
		let capacity = ringbuffer_metadata::SCHEMA.get_u64(&row, ringbuffer_metadata::CAPACITY);
		let head = ringbuffer_metadata::SCHEMA.get_u64(&row, ringbuffer_metadata::HEAD);
		let tail = ringbuffer_metadata::SCHEMA.get_u64(&row, ringbuffer_metadata::TAIL);
		let current_size = ringbuffer_metadata::SCHEMA.get_u64(&row, ringbuffer_metadata::COUNT);

		Ok(Some(RingBufferMetadata {
			id: buffer_id,
			capacity,
			count: current_size,
			head,
			tail,
		}))
	}

	pub(crate) fn find_ringbuffer_by_name(
		rx: &mut impl AsTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<RingBufferDef>> {
		let name = name.as_ref();
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(NamespaceRingBufferKey::full_scan(namespace), 1024)?;

		let mut found_ringbuffer = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let ringbuffer_name = ringbuffer_namespace::SCHEMA.get_utf8(row, ringbuffer_namespace::NAME);
			if name == ringbuffer_name {
				found_ringbuffer = Some(RingBufferId(
					ringbuffer_namespace::SCHEMA.get_u64(row, ringbuffer_namespace::ID),
				));
				break;
			}
		}

		drop(stream);

		let Some(ringbuffer) = found_ringbuffer else {
			return Ok(None);
		};

		Ok(Some(Self::get_ringbuffer(&mut txn, ringbuffer)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, RingBufferId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{
		CatalogStore,
		store::{
			namespace::create::NamespaceToCreate,
			primary_key::create::PrimaryKeyToCreate,
			ringbuffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
		},
		test_utils::{ensure_test_namespace, ensure_test_ringbuffer},
	};

	#[test]
	fn test_find_ringbuffer_exists() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let found = CatalogStore::find_ringbuffer(&mut txn, ringbuffer.id)
			.unwrap()
			.expect("Ring buffer should exist");

		assert_eq!(found.id, ringbuffer.id);
		assert_eq!(found.name, ringbuffer.name);
		assert_eq!(found.namespace, ringbuffer.namespace);
		assert_eq!(found.capacity, ringbuffer.capacity);
	}

	#[test]
	fn test_find_ringbuffer_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_ringbuffer(&mut txn, RingBufferId(999)).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_metadata() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let metadata = CatalogStore::find_ringbuffer_metadata(&mut txn, ringbuffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.id, ringbuffer.id);
		assert_eq!(metadata.capacity, ringbuffer.capacity);
		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);
	}

	#[test]
	fn test_find_ringbuffer_metadata_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_ringbuffer_metadata(&mut txn, RingBufferId(999)).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_by_name_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create a ring buffer with specific name
		let to_create = RingBufferToCreate {
			namespace: namespace.id,
			ringbuffer: "trades_buffer".to_string(),
			capacity: 200,
			columns: vec![RingBufferColumnToCreate {
				name: "symbol".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Utf8),
				fragment: None,
				policies: vec![],
				auto_increment: false,
				dictionary_id: None,
			}],
			fragment: None,
		};

		let created = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Find by name
		let found = CatalogStore::find_ringbuffer_by_name(&mut txn, namespace.id, "trades_buffer")
			.unwrap()
			.expect("Should find ring buffer by name");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, "trades_buffer");
		assert_eq!(found.capacity, 200);
		assert_eq!(found.columns.len(), 1);
	}

	#[test]
	fn test_find_ringbuffer_by_name_not_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result =
			CatalogStore::find_ringbuffer_by_name(&mut txn, namespace.id, "nonexistent_buffer").unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create namespace2
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Create ring buffer in namespace1
		let to_create = RingBufferToCreate {
			namespace: namespace1.id,
			ringbuffer: "shared_name".to_string(),
			capacity: 50,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Try to find in namespace2 - should not exist
		let result = CatalogStore::find_ringbuffer_by_name(&mut txn, namespace2.id, "shared_name").unwrap();

		assert!(result.is_none());

		// Find in namespace1 - should exist
		let found = CatalogStore::find_ringbuffer_by_name(&mut txn, namespace1.id, "shared_name").unwrap();

		assert!(found.is_some());
	}

	#[test]
	fn test_find_ringbuffer_with_columns_and_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create ring buffer with columns
		let to_create = RingBufferToCreate {
			namespace: namespace.id,
			ringbuffer: "pk_buffer".to_string(),
			capacity: 100,
			columns: vec![
				RingBufferColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					fragment: None,
					policies: vec![],
					auto_increment: true,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: "value".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Float8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
			fragment: None,
		};

		let created = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		// Add primary key
		let columns = CatalogStore::list_columns(&mut txn, created.id).unwrap();
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: created.id.into(),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Find and verify
		let found =
			CatalogStore::find_ringbuffer(&mut txn, created.id).unwrap().expect("Ring buffer should exist");

		assert_eq!(found.columns.len(), 2);
		assert_eq!(found.columns[0].name, "id");
		assert_eq!(found.columns[0].auto_increment, true);
		assert_eq!(found.columns[1].name, "value");
		assert!(found.primary_key.is_some());
		assert_eq!(found.primary_key.unwrap().id, pk_id);
	}
}
