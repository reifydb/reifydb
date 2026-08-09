// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		id::{NamespaceId, RingBufferId},
		ringbuffer::RingBuffer,
	},
	key::{Key, ringbuffer::RingBufferKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::ringbuffer::{decode_ringbuffer_time, shape::ringbuffer},
};

type RingBufferRow = (RingBufferId, NamespaceId, String, u64, Vec<String>, bool, TimeSource);

impl CatalogStore {
	pub(crate) fn list_ringbuffers_all(rx: &mut Transaction<'_>) -> Result<Vec<RingBuffer>> {
		let mut result = Vec::new();

		let mut ringbuffer_data: Vec<RingBufferRow> = Vec::new();
		{
			let stream = rx.range(RingBufferKey::full_scan(), RangeScope::All, 1024)?;

			for entry in stream {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key)
					&& let Key::RingBuffer(ringbuffer_key) = key
				{
					let ringbuffer_id = ringbuffer_key.ringbuffer;

					let namespace_id = NamespaceId(ringbuffer::get_namespace(
						EncodedCatalogRow::view(&entry.bytes),
					));

					let name =
						ringbuffer::get_name(EncodedCatalogRow::view(&entry.bytes)).to_string();

					let capacity = ringbuffer::get_capacity(EncodedCatalogRow::view(&entry.bytes));

					let partition_by_str =
						ringbuffer::get_partition_by(EncodedCatalogRow::view(&entry.bytes));
					let partition_by = if partition_by_str.is_empty() {
						vec![]
					} else {
						partition_by_str.split(',').map(|s| s.to_string()).collect()
					};

					let underlying =
						ringbuffer::get_underlying(EncodedCatalogRow::view(&entry.bytes)) != 0;

					let time = decode_ringbuffer_time(EncodedCatalogRow::view(&entry.bytes));

					ringbuffer_data.push((
						ringbuffer_id,
						namespace_id,
						name,
						capacity,
						partition_by,
						underlying,
						time,
					));
				}
			}
		}

		for (ringbuffer_id, namespace_id, name, capacity, partition_by, underlying, time) in ringbuffer_data {
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
				underlying,
				time,
			};

			result.push(ringbuffer);
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{common::TimeSource, interface::catalog::id::NamespaceId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::fragment::Fragment;

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

		let buffer1 = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("buffer1"),
			capacity: 100,
			columns: vec![],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).unwrap();

		let buffer2 = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("buffer2"),
			capacity: 200,
			columns: vec![],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
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

		let buffer1 = RingBufferToCreate {
			namespace: namespace1.id(),
			name: Fragment::internal("buffer1"),
			capacity: 100,
			columns: vec![],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer1).unwrap();

		let buffer2 = RingBufferToCreate {
			namespace: namespace2.id(),
			name: Fragment::internal("buffer2"),
			capacity: 200,
			columns: vec![],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};
		CatalogStore::create_ringbuffer(&mut txn, buffer2).unwrap();

		let all_buffers = CatalogStore::list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all_buffers.len(), 2);

		let buffer1_entry = all_buffers.iter().find(|b| b.name == "buffer1").expect("buffer1 should exist");
		assert_eq!(buffer1_entry.namespace, namespace1.id());

		let buffer2_entry = all_buffers.iter().find(|b| b.name == "buffer2").expect("buffer2 should exist");
		assert_eq!(buffer2_entry.namespace, namespace2.id());
	}
}
