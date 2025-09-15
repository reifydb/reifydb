// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, NamespaceId, NamespaceRingBufferKey, QueryTransaction,
	RingBufferDef, RingBufferId, RingBufferKey, RingBufferMetadata,
	RingBufferMetadataKey, Versioned,
};

use crate::{
	CatalogStore,
	ring_buffer::layout::{
		ring_buffer, ring_buffer_metadata, ring_buffer_namespace,
	},
};

impl CatalogStore {
	pub fn find_ring_buffer(
		rx: &mut impl QueryTransaction,
		ring_buffer: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		let Some(versioned) =
			rx.get(&RingBufferKey::new(ring_buffer).encode())?
		else {
			return Ok(None);
		};

		let row = versioned.row;
		let id = RingBufferId(
			ring_buffer::LAYOUT.get_u64(&row, ring_buffer::ID),
		);
		let namespace = NamespaceId(
			ring_buffer::LAYOUT
				.get_u64(&row, ring_buffer::NAMESPACE),
		);
		let name = ring_buffer::LAYOUT
			.get_utf8(&row, ring_buffer::NAME)
			.to_string();
		let capacity = ring_buffer::LAYOUT
			.get_u64(&row, ring_buffer::CAPACITY);

		Ok(Some(RingBufferDef {
			id,
			namespace,
			name,
			capacity,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_primary_key(rx, id)?,
		}))
	}

	pub fn find_ring_buffer_metadata(
		rx: &mut impl QueryTransaction,
		ring_buffer: RingBufferId,
	) -> crate::Result<Option<RingBufferMetadata>> {
		let Some(versioned) = rx
			.get(&RingBufferMetadataKey::new(ring_buffer)
				.encode())?
		else {
			return Ok(None);
		};

		let row = versioned.row;
		let buffer_id = RingBufferId(
			ring_buffer_metadata::LAYOUT
				.get_u64(&row, ring_buffer_metadata::ID),
		);
		let capacity = ring_buffer_metadata::LAYOUT
			.get_u64(&row, ring_buffer_metadata::CAPACITY);
		let head = ring_buffer_metadata::LAYOUT
			.get_u64(&row, ring_buffer_metadata::HEAD);
		let tail = ring_buffer_metadata::LAYOUT
			.get_u64(&row, ring_buffer_metadata::TAIL);
		let current_size = ring_buffer_metadata::LAYOUT
			.get_u64(&row, ring_buffer_metadata::COUNT);

		Ok(Some(RingBufferMetadata {
			id: buffer_id,
			capacity,
			current_size,
			head,
			tail,
		}))
	}

	pub fn find_ring_buffer_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<RingBufferDef>> {
		let name = name.as_ref();
		let Some(ring_buffer) =
			rx.range(NamespaceRingBufferKey::full_scan(namespace))?
				.find_map(|versioned: Versioned| {
					let row = &versioned.row;
					let ring_buffer_name = ring_buffer_namespace::LAYOUT
					.get_utf8(row, ring_buffer_namespace::NAME);
					if name == ring_buffer_name {
						Some(RingBufferId(ring_buffer_namespace::LAYOUT
						.get_u64(
							row,
							ring_buffer_namespace::ID,
						)))
					} else {
						None
					}
				})
		else {
			return Ok(None);
		};

		Ok(Some(Self::get_ring_buffer(rx, ring_buffer)?))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::RingBufferId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		ring_buffer::create::{
			RingBufferColumnToCreate, RingBufferToCreate,
		},
		test_utils::{ensure_test_namespace, ensure_test_ring_buffer},
	};

	#[test]
	fn test_find_ring_buffer_exists() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let found = CatalogStore::find_ring_buffer(
			&mut txn,
			ring_buffer.id,
		)
		.unwrap()
		.expect("Ring buffer should exist");

		assert_eq!(found.id, ring_buffer.id);
		assert_eq!(found.name, ring_buffer.name);
		assert_eq!(found.namespace, ring_buffer.namespace);
		assert_eq!(found.capacity, ring_buffer.capacity);
	}

	#[test]
	fn test_find_ring_buffer_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_ring_buffer(
			&mut txn,
			RingBufferId(999),
		)
		.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ring_buffer_metadata() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let metadata = CatalogStore::find_ring_buffer_metadata(
			&mut txn,
			ring_buffer.id,
		)
		.unwrap()
		.expect("Metadata should exist");

		assert_eq!(metadata.id, ring_buffer.id);
		assert_eq!(metadata.capacity, ring_buffer.capacity);
		assert_eq!(metadata.current_size, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);
	}

	#[test]
	fn test_find_ring_buffer_metadata_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_ring_buffer_metadata(
			&mut txn,
			RingBufferId(999),
		)
		.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ring_buffer_by_name_exists() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create a ring buffer with specific name
		let to_create = RingBufferToCreate {
			namespace: namespace.id,
			ring_buffer: "trades_buffer".to_string(),
			capacity: 200,
			columns: vec![RingBufferColumnToCreate {
				name: "symbol".to_string(),
				constraint: TypeConstraint::unconstrained(
					Type::Utf8,
				),
				fragment: None,
				policies: vec![],
				auto_increment: false,
			}],
			fragment: None,
		};

		let created =
			CatalogStore::create_ring_buffer(&mut txn, to_create)
				.unwrap();

		// Find by name
		let found = CatalogStore::find_ring_buffer_by_name(
			&mut txn,
			namespace.id,
			"trades_buffer",
		)
		.unwrap()
		.expect("Should find ring buffer by name");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, "trades_buffer");
		assert_eq!(found.capacity, 200);
		assert_eq!(found.columns.len(), 1);
	}

	#[test]
	fn test_find_ring_buffer_by_name_not_exists() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_ring_buffer_by_name(
			&mut txn,
			namespace.id,
			"nonexistent_buffer",
		)
		.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ring_buffer_by_name_different_namespace() {
		let mut txn = create_test_command_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create namespace2
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			crate::namespace::NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.unwrap();

		// Create ring buffer in namespace1
		let to_create = RingBufferToCreate {
			namespace: namespace1.id,
			ring_buffer: "shared_name".to_string(),
			capacity: 50,
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_ring_buffer(&mut txn, to_create).unwrap();

		// Try to find in namespace2 - should not exist
		let result = CatalogStore::find_ring_buffer_by_name(
			&mut txn,
			namespace2.id,
			"shared_name",
		)
		.unwrap();

		assert!(result.is_none());

		// Find in namespace1 - should exist
		let found = CatalogStore::find_ring_buffer_by_name(
			&mut txn,
			namespace1.id,
			"shared_name",
		)
		.unwrap();

		assert!(found.is_some());
	}

	#[test]
	fn test_find_ring_buffer_with_columns_and_primary_key() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create ring buffer with columns
		let to_create =
			RingBufferToCreate {
				namespace: namespace.id,
				ring_buffer: "pk_buffer".to_string(),
				capacity: 100,
				columns: vec![
				RingBufferColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					fragment: None,
					policies: vec![],
					auto_increment: true,
				},
				RingBufferColumnToCreate {
					name: "value".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Float8),
					fragment: None,
					policies: vec![],
					auto_increment: false,
				},
			],
				fragment: None,
			};

		let created =
			CatalogStore::create_ring_buffer(&mut txn, to_create)
				.unwrap();

		// Add primary key
		let columns = CatalogStore::list_columns(&mut txn, created.id)
			.unwrap();
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			crate::primary_key::PrimaryKeyToCreate {
				source: created.id.into(),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Find and verify
		let found =
			CatalogStore::find_ring_buffer(&mut txn, created.id)
				.unwrap()
				.expect("Ring buffer should exist");

		assert_eq!(found.columns.len(), 2);
		assert_eq!(found.columns[0].name, "id");
		assert_eq!(found.columns[0].auto_increment, true);
		assert_eq!(found.columns[1].name, "value");
		assert!(found.primary_key.is_some());
		assert_eq!(found.primary_key.unwrap().id, pk_id);
	}
}
