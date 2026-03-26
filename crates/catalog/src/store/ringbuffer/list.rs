// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, RingBufferId},
		ringbuffer::RingBuffer,
	},
	key::{Key, ringbuffer::RingBufferKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::ringbuffer::schema::ringbuffer};

impl CatalogStore {
	pub(crate) fn list_ringbuffers_all(rx: &mut Transaction<'_>) -> Result<Vec<RingBuffer>> {
		let mut result = Vec::new();

		// Collect ringbuffer data first to avoid holding stream borrow
		let mut ringbuffer_data: Vec<(RingBufferId, NamespaceId, String, u64, Vec<String>)> = Vec::new();
		{
			let mut stream = rx.range(RingBufferKey::full_scan(), 1024)?;

			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key) {
					if let Key::RingBuffer(ringbuffer_key) = key {
						let ringbuffer_id = ringbuffer_key.ringbuffer;

						let namespace_id = NamespaceId(
							ringbuffer::SCHEMA.get_u64(&entry.row, ringbuffer::NAMESPACE),
						);

						let name = ringbuffer::SCHEMA
							.get_utf8(&entry.row, ringbuffer::NAME)
							.to_string();

						let capacity =
							ringbuffer::SCHEMA.get_u64(&entry.row, ringbuffer::CAPACITY);

						let partition_by_str = ringbuffer::SCHEMA
							.get_utf8(&entry.row, ringbuffer::PARTITION_BY);
						let partition_by = if partition_by_str.is_empty() {
							vec![]
						} else {
							partition_by_str.split(',').map(|s| s.to_string()).collect()
						};

						ringbuffer_data.push((
							ringbuffer_id,
							namespace_id,
							name,
							capacity,
							partition_by,
						));
					}
				}
			}
		}

		// Now fetch additional details for each ringbuffer
		for (ringbuffer_id, namespace_id, name, capacity, partition_by) in ringbuffer_data {
			let primary_key = Self::find_primary_key(rx, ringbuffer_id)?;
			let columns = Self::list_columns(rx, ringbuffer_id)?;

			let ringbuffer = RingBuffer {
				id: ringbuffer_id,
				namespace: namespace_id,
				name,
				capacity,
				columns,
				primary_key,
				partition_by,
			};

			result.push(ringbuffer);
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::{namespace::create::NamespaceToCreate, ringbuffer::create::RingBufferToCreate},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_list_ringbuffers_empty() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let buffers = CatalogStore::list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(buffers.len(), 0);
	}

	#[test]
	fn test_list_ringbuffers_multiple() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create first ring buffer
		let buffer1 = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("buffer1"),
			capacity: 100,
			columns: vec![],
			partition_by: vec![],
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).unwrap();

		// Create second ring buffer
		let buffer2 = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("buffer2"),
			capacity: 200,
			columns: vec![],
			partition_by: vec![],
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer2).unwrap();

		let buffers = CatalogStore::list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(buffers.len(), 2);
		assert!(buffers.iter().any(|b| b.name == "buffer1"));
		assert!(buffers.iter().any(|b| b.name == "buffer2"));
	}

	#[test]
	fn test_list_ringbuffers_different_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create second namespace
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
				local_name: "namespace2".to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.unwrap();

		// Create buffer in namespace1
		let buffer1 = RingBufferToCreate {
			namespace: namespace1.id(),
			name: Fragment::internal("buffer1"),
			capacity: 100,
			columns: vec![],
			partition_by: vec![],
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).unwrap();

		// Create buffer in namespace2
		let buffer2 = RingBufferToCreate {
			namespace: namespace2.id(),
			name: Fragment::internal("buffer2"),
			capacity: 200,
			columns: vec![],
			partition_by: vec![],
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer2).unwrap();

		// List all buffers
		let all_buffers = CatalogStore::list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all_buffers.len(), 2);

		// Check that buffer1 is in namespace1
		let buffer1_entry = all_buffers.iter().find(|b| b.name == "buffer1").expect("buffer1 should exist");
		assert_eq!(buffer1_entry.namespace, namespace1.id());

		// Check that buffer2 is in namespace2
		let buffer2_entry = all_buffers.iter().find(|b| b.name == "buffer2").expect("buffer2 should exist");
		assert_eq!(buffer2_entry.namespace, namespace2.id());
	}
}
