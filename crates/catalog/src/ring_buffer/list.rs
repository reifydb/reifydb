// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceId, QueryTransaction, RingBufferDef, RingBufferKey};

use crate::{CatalogStore, ring_buffer::layout::ring_buffer};

impl CatalogStore {
	pub fn list_ring_buffers_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<RingBufferDef>> {
		let mut result = Vec::new();

		let entries: Vec<_> = rx.range(RingBufferKey::full_scan())?.into_iter().collect();

		for entry in entries {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::RingBuffer(ring_buffer_key) = key {
					let ring_buffer_id = ring_buffer_key.ring_buffer;

					let namespace_id = NamespaceId(
						ring_buffer::LAYOSVT.get_u64(&entry.row, ring_buffer::NAMESPACE),
					);

					let name = ring_buffer::LAYOSVT
						.get_utf8(&entry.row, ring_buffer::NAME)
						.to_string();

					let capacity = ring_buffer::LAYOSVT.get_u64(&entry.row, ring_buffer::CAPACITY);

					let primary_key = Self::find_primary_key(rx, ring_buffer_id)?;
					let columns = Self::list_columns(rx, ring_buffer_id)?;

					let ring_buffer_def = RingBufferDef {
						id: ring_buffer_id,
						namespace: namespace_id,
						name,
						capacity,
						columns,
						primary_key,
					};

					result.push(ring_buffer_def);
				}
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, ring_buffer::create::RingBufferToCreate, test_utils::ensure_test_namespace};

	#[test]
	fn test_list_ring_buffers_empty() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);

		let buffers = CatalogStore::list_ring_buffers_all(&mut txn).unwrap();

		assert_eq!(buffers.len(), 0);
	}

	#[test]
	fn test_list_ring_buffers_multiple() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create first ring buffer
		let buffer1 = RingBufferToCreate {
			namespace: namespace.id,
			ring_buffer: "buffer1".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ring_buffer(&mut txn, buffer1).unwrap();

		// Create second ring buffer
		let buffer2 = RingBufferToCreate {
			namespace: namespace.id,
			ring_buffer: "buffer2".to_string(),
			capacity: 200,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ring_buffer(&mut txn, buffer2).unwrap();

		let buffers = CatalogStore::list_ring_buffers_all(&mut txn).unwrap();

		assert_eq!(buffers.len(), 2);
		assert!(buffers.iter().any(|b| b.name == "buffer1"));
		assert!(buffers.iter().any(|b| b.name == "buffer2"));
	}

	#[test]
	fn test_list_ring_buffers_different_namespaces() {
		let mut txn = create_test_command_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create second namespace
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			crate::namespace::NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.unwrap();

		// Create buffer in namespace1
		let buffer1 = RingBufferToCreate {
			namespace: namespace1.id,
			ring_buffer: "buffer1".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ring_buffer(&mut txn, buffer1).unwrap();

		// Create buffer in namespace2
		let buffer2 = RingBufferToCreate {
			namespace: namespace2.id,
			ring_buffer: "buffer2".to_string(),
			capacity: 200,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ring_buffer(&mut txn, buffer2).unwrap();

		// List all buffers
		let all_buffers = CatalogStore::list_ring_buffers_all(&mut txn).unwrap();
		assert_eq!(all_buffers.len(), 2);

		// Check that buffer1 is in namespace1
		let buffer1_entry = all_buffers.iter().find(|b| b.name == "buffer1").expect("buffer1 should exist");
		assert_eq!(buffer1_entry.namespace, namespace1.id);

		// Check that buffer2 is in namespace2
		let buffer2_entry = all_buffers.iter().find(|b| b.name == "buffer2").expect("buffer2 should exist");
		assert_eq!(buffer2_entry.namespace, namespace2.id);
	}
}
