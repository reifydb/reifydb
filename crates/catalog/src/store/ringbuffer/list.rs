// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{Key, NamespaceId, RingBufferDef, RingBufferKey};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::ringbuffer::layout::ringbuffer};

impl CatalogStore {
	pub async fn list_ringbuffers_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<RingBufferDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let batch = txn.range_batch(RingBufferKey::full_scan(), 1024).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::RingBuffer(ringbuffer_key) = key {
					let ringbuffer_id = ringbuffer_key.ringbuffer;

					let namespace_id = NamespaceId(
						ringbuffer::LAYOUT.get_u64(&entry.values, ringbuffer::NAMESPACE),
					);

					let name = ringbuffer::LAYOUT
						.get_utf8(&entry.values, ringbuffer::NAME)
						.to_string();

					let capacity = ringbuffer::LAYOUT.get_u64(&entry.values, ringbuffer::CAPACITY);

					let primary_key = Self::find_primary_key(&mut txn, ringbuffer_id).await?;
					let columns = Self::list_columns(&mut txn, ringbuffer_id).await?;

					let ringbuffer_def = RingBufferDef {
						id: ringbuffer_id,
						namespace: namespace_id,
						name,
						capacity,
						columns,
						primary_key,
					};

					result.push(ringbuffer_def);
				}
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore, namespace::NamespaceToCreate, ringbuffer::create::RingBufferToCreate,
		test_utils::ensure_test_namespace,
	};

	#[tokio::test]
	async fn test_list_ringbuffers_empty() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;

		let buffers = CatalogStore::list_ringbuffers_all(&mut txn).await.unwrap();

		assert_eq!(buffers.len(), 0);
	}

	#[tokio::test]
	async fn test_list_ringbuffers_multiple() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

		// Create first ring buffer
		let buffer1 = RingBufferToCreate {
			namespace: namespace.id,
			ringbuffer: "buffer1".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).await.unwrap();

		// Create second ring buffer
		let buffer2 = RingBufferToCreate {
			namespace: namespace.id,
			ringbuffer: "buffer2".to_string(),
			capacity: 200,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer2).await.unwrap();

		let buffers = CatalogStore::list_ringbuffers_all(&mut txn).await.unwrap();

		assert_eq!(buffers.len(), 2);
		assert!(buffers.iter().any(|b| b.name == "buffer1"));
		assert!(buffers.iter().any(|b| b.name == "buffer2"));
	}

	#[tokio::test]
	async fn test_list_ringbuffers_different_namespaces() {
		let mut txn = create_test_command_transaction().await;
		let namespace1 = ensure_test_namespace(&mut txn).await;

		// Create second namespace
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.await
		.unwrap();

		// Create buffer in namespace1
		let buffer1 = RingBufferToCreate {
			namespace: namespace1.id,
			ringbuffer: "buffer1".to_string(),
			capacity: 100,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).await.unwrap();

		// Create buffer in namespace2
		let buffer2 = RingBufferToCreate {
			namespace: namespace2.id,
			ringbuffer: "buffer2".to_string(),
			capacity: 200,
			columns: vec![],
			fragment: None,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer2).await.unwrap();

		// List all buffers
		let all_buffers = CatalogStore::list_ringbuffers_all(&mut txn).await.unwrap();
		assert_eq!(all_buffers.len(), 2);

		// Check that buffer1 is in namespace1
		let buffer1_entry = all_buffers.iter().find(|b| b.name == "buffer1").expect("buffer1 should exist");
		assert_eq!(buffer1_entry.namespace, namespace1.id);

		// Check that buffer2 is in namespace2
		let buffer2_entry = all_buffers.iter().find(|b| b.name == "buffer2").expect("buffer2 should exist");
		assert_eq!(buffer2_entry.namespace, namespace2.id);
	}
}
