// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::deserializer::KeyDeserializer,
	row::{catalog::EncodedCatalogRow, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, RingBufferId},
		ringbuffer::{PartitionedMetadata, RingBuffer, RingBufferMetadata, decode_ringbuffer_metadata},
	},
	key::{
		catalog::KeyDeserializerCatalogExt,
		namespace_ringbuffer::NamespaceRingBufferKey,
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::Value;

use crate::{
	CatalogStore, Result,
	store::ringbuffer::{
		decode_ringbuffer_time,
		shape::{ringbuffer, ringbuffer_namespace},
	},
};

impl CatalogStore {
	pub(crate) fn find_ringbuffer(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
	) -> Result<Option<RingBuffer>> {
		let Some(multi) = rx.get(&RingBufferKey::encoded(ringbuffer))? else {
			return Ok(None);
		};

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
		let id = RingBufferId(ringbuffer::get_id(&bytes));
		let namespace = NamespaceId(ringbuffer::get_namespace(&bytes));
		let name = ringbuffer::get_name(&bytes).to_string();
		let capacity = ringbuffer::get_capacity(&bytes);

		let partition_by_str = ringbuffer::get_partition_by(&bytes);
		let partition_by = if partition_by_str.is_empty() {
			vec![]
		} else {
			partition_by_str.split(',').map(|s| s.to_string()).collect()
		};
		let underlying = ringbuffer::get_underlying(&bytes) != 0;

		Ok(Some(RingBuffer {
			id,
			namespace,
			name,
			capacity,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_primary_key(rx, id)?,
			partition_by,
			underlying,
			time: decode_ringbuffer_time(&bytes),
		}))
	}

	pub(crate) fn find_ringbuffer_metadata(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
	) -> Result<Option<RingBufferMetadata>> {
		let Some(multi) = rx.get(&RingBufferMetadataKey::encoded(ringbuffer))? else {
			return Ok(None);
		};

		Ok(Some(decode_ringbuffer_metadata(EncodedPodRow::view(&multi.bytes))?))
	}

	pub(crate) fn find_ringbuffer_partition_metadata(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
	) -> Result<Option<RingBufferMetadata>> {
		let key = RingBufferMetadataKey::encoded_partition(ringbuffer, partition_values.to_vec());
		let Some(multi) = rx.get(&key)? else {
			return Ok(None);
		};

		Ok(Some(decode_ringbuffer_metadata(EncodedPodRow::view(&multi.bytes))?))
	}

	pub(crate) fn list_ringbuffer_partition_metadata(
		rx: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
	) -> Result<Vec<PartitionedMetadata>> {
		let range = RingBufferMetadataKey::full_scan_for_storage(ringbuffer.id);
		let stream = rx.range(range, RangeScope::All, 4096)?;
		let mut results = Vec::new();

		for entry in stream {
			let multi = entry?;
			let metadata = decode_ringbuffer_metadata(EncodedPodRow::view(&multi.bytes))?;
			let mut de = KeyDeserializer::from_bytes(multi.key.as_slice());

			let _ = (de.read_u8(), de.read_object_id());
			let mut partition_values = vec![];
			while !de.is_empty() {
				if let Ok(value) = de.read_value() {
					partition_values.push(value);
				} else {
					break;
				}
			}
			results.push(PartitionedMetadata {
				metadata,
				partition_values,
			});
		}

		Ok(results)
	}

	pub(crate) fn list_ringbuffer_partitions(
		rx: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
	) -> Result<Vec<PartitionedMetadata>> {
		if ringbuffer.partition_by.is_empty() {
			Ok(Self::find_ringbuffer_metadata(rx, ringbuffer.id)?
				.into_iter()
				.map(|metadata| PartitionedMetadata {
					metadata,
					partition_values: vec![],
				})
				.collect())
		} else {
			Self::list_ringbuffer_partition_metadata(rx, ringbuffer)
		}
	}

	pub(crate) fn find_partition_metadata(
		rx: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition_key: &[Value],
	) -> Result<Option<RingBufferMetadata>> {
		if ringbuffer.partition_by.is_empty() {
			Self::find_ringbuffer_metadata(rx, ringbuffer.id)
		} else {
			Self::find_ringbuffer_partition_metadata(rx, ringbuffer.id, partition_key)
		}
	}

	pub(crate) fn find_ringbuffer_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<RingBuffer>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceRingBufferKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_ringbuffer = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = EncodedCatalogRow::view(&multi.bytes);
			let ringbuffer_name = ringbuffer_namespace::get_name(bytes);
			if name == ringbuffer_name {
				found_ringbuffer = Some(RingBufferId(ringbuffer_namespace::get_id(bytes)));
				break;
			}
		}

		drop(stream);

		let Some(ringbuffer) = found_ringbuffer else {
			return Ok(None);
		};

		Ok(Some(Self::get_ringbuffer(rx, ringbuffer)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::id::{NamespaceId, RingBufferId},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

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

		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), ringbuffer.id)
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

		let result =
			CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), RingBufferId(999)).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_metadata() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let metadata = CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 1);
		assert_eq!(metadata.tail, 1);
	}

	#[test]
	fn test_find_ringbuffer_metadata_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result =
			CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), RingBufferId(999))
				.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_by_name_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("trades_buffer"),
			capacity: 200,
			columns: vec![RingBufferColumnToCreate {
				name: Fragment::internal("symbol"),
				fragment: Fragment::None,
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				auto_increment: false,
				dictionary_id: None,
			}],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};

		let created = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		let found = CatalogStore::find_ringbuffer_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id(),
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
	fn test_find_ringbuffer_by_name_not_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_ringbuffer_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id(),
			"nonexistent_buffer",
		)
		.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_ringbuffer_by_name_different_namespace() {
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

		let to_create = RingBufferToCreate {
			namespace: namespace1.id(),
			name: Fragment::internal("shared_name"),
			capacity: 50,
			columns: vec![],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};

		CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		let result = CatalogStore::find_ringbuffer_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace2.id(),
			"shared_name",
		)
		.unwrap();

		assert!(result.is_none());

		let found = CatalogStore::find_ringbuffer_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace1.id(),
			"shared_name",
		)
		.unwrap();

		assert!(found.is_some());
	}

	#[test]
	fn test_find_ringbuffer_with_columns_and_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = RingBufferToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("pk_buffer"),
			capacity: 100,
			columns: vec![
				RingBufferColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					auto_increment: true,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: Fragment::internal("value"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Float8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		};

		let created = CatalogStore::create_ringbuffer(&mut txn, to_create).unwrap();

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				object: created.id.into(),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id)
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
