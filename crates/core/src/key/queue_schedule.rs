// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::QueueId;

fn queue_prefix(kind: KeyKind, queue: QueueId) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(9);
	serializer.extend_u8(kind as u8).extend_u64(queue);
	serializer.to_encoded_key()
}

fn partition_prefix(kind: KeyKind, queue: QueueId, partition: u16) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(11);
	serializer.extend_u8(kind as u8).extend_u64(queue).extend_u16(partition);
	serializer.to_encoded_key()
}

fn key_prefix(kind: KeyKind, queue: QueueId, partition: u16, key_hash: u64) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(20);
	serializer.extend_u8(kind as u8).extend_u64(queue).extend_u16(partition).extend_u64(key_hash);
	serializer.to_encoded_key()
}

fn family_scan(kind: KeyKind) -> EncodedKeyRange {
	let mut start = KeySerializer::with_capacity(1);
	start.extend_u8(kind as u8);
	let mut end = KeySerializer::with_capacity(1);
	end.extend_u8(kind as u8 - 1);
	EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuePartitionKey {
	pub queue: QueueId,
	pub partition: u16,
}

impl EncodableKey for QueuePartitionKey {
	const KIND: KeyKind = KeyKind::QueuePartition;

	fn encode(&self) -> EncodedKey {
		partition_prefix(Self::KIND, self.queue, self.partition)
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let partition = de.read_u16().ok()?;

		Some(Self {
			queue: QueueId(queue),
			partition,
		})
	}
}

impl QueuePartitionKey {
	pub fn encoded(queue: impl Into<QueueId>, partition: u16) -> EncodedKey {
		Self {
			queue: queue.into(),
			partition,
		}
		.encode()
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(Self::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(Self::KIND)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueItemStateKey {
	pub queue: QueueId,
	pub partition: u16,
	pub row: RowNumber,
}

impl EncodableKey for QueueItemStateKey {
	const KIND: KeyKind = KeyKind::QueueItemState;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.queue)
			.extend_u16(self.partition)
			.extend_u64(self.row.0);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let partition = de.read_u16().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			queue: QueueId(queue),
			partition,
			row,
		})
	}
}

impl QueueItemStateKey {
	pub fn encoded(queue: impl Into<QueueId>, partition: u16, row: impl Into<RowNumber>) -> EncodedKey {
		Self {
			queue: queue.into(),
			partition,
			row: row.into(),
		}
		.encode()
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(Self::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(Self::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(Self::KIND)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDueKey {
	pub queue: QueueId,
	pub partition: u16,
	pub due: DateTime,
	pub row: RowNumber,
}

impl EncodableKey for QueueDueKey {
	const KIND: KeyKind = KeyKind::QueueDue;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(27);
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.queue)
			.extend_u16(self.partition)
			.extend_datetime(&self.due)
			.extend_u64(self.row.0);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let partition = de.read_u16().ok()?;
		let due = de.read_datetime().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			queue: QueueId(queue),
			partition,
			due,
			row,
		})
	}
}

impl QueueDueKey {
	pub fn encoded(
		queue: impl Into<QueueId>,
		partition: u16,
		due: DateTime,
		row: impl Into<RowNumber>,
	) -> EncodedKey {
		Self {
			queue: queue.into(),
			partition,
			due,
			row: row.into(),
		}
		.encode()
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(Self::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(Self::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(Self::KIND)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueKeyActiveKey {
	pub queue: QueueId,
	pub partition: u16,
	pub key_hash: u64,
	pub row: RowNumber,
}

impl EncodableKey for QueueKeyActiveKey {
	const KIND: KeyKind = KeyKind::QueueKeyActive;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(28);
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.queue)
			.extend_u16(self.partition)
			.extend_u64(self.key_hash)
			.extend_u64(self.row.0);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let partition = de.read_u16().ok()?;
		let key_hash = de.read_u64().ok()?;
		let row = de.read_row_number().ok()?;

		Some(Self {
			queue: QueueId(queue),
			partition,
			key_hash,
			row,
		})
	}
}

impl QueueKeyActiveKey {
	pub fn encoded(
		queue: impl Into<QueueId>,
		partition: u16,
		key_hash: u64,
		row: impl Into<RowNumber>,
	) -> EncodedKey {
		Self {
			queue: queue.into(),
			partition,
			key_hash,
			row: row.into(),
		}
		.encode()
	}

	pub fn key_scan(queue: QueueId, partition: u16, key_hash: u64) -> EncodedKeyRange {
		EncodedKeyRange::prefix(key_prefix(Self::KIND, queue, partition, key_hash).as_slice())
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(Self::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(Self::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(Self::KIND)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::*;

	fn contains(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
		let after_start = match &range.start {
			Bound::Included(start) => key.as_slice() >= start.as_slice(),
			Bound::Excluded(start) => key.as_slice() > start.as_slice(),
			Bound::Unbounded => true,
		};
		let before_end = match &range.end {
			Bound::Included(end) => key.as_slice() <= end.as_slice(),
			Bound::Excluded(end) => key.as_slice() < end.as_slice(),
			Bound::Unbounded => true,
		};
		after_start && before_end
	}

	#[test]
	fn test_partition_key_roundtrips_at_both_partition_bounds() {
		// The counter row is addressed by this key and is also the lock key every claim
		// serialises on. A codec that loses the partition would point two partitions at one
		// counter, so depth accounting and the lock would silently merge.
		for partition in [0u16, 1, 1023] {
			let encoded = QueuePartitionKey::encoded(QueueId(7), partition);
			let decoded = QueuePartitionKey::decode(&encoded).unwrap();
			assert_eq!(decoded.queue, QueueId(7));
			assert_eq!(decoded.partition, partition);
		}
	}

	#[test]
	fn test_item_state_key_roundtrips() {
		// The state record is the compare-and-set target for every transition; a key that
		// decodes to the wrong row would transition somebody else's item.
		let encoded = QueueItemStateKey::encoded(QueueId(3), 5, RowNumber(42));
		let decoded = QueueItemStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(3));
		assert_eq!(decoded.partition, 5);
		assert_eq!(decoded.row, RowNumber(42));
	}

	#[test]
	fn test_due_key_roundtrips_at_epoch_and_far_future() {
		// Epoch is the due time of every immediately-ready item, and a far-future value is
		// what a long not_before produces; both ends must survive the varint encoding or the
		// due index would resolve to the wrong instant.
		for nanos in [0u64, 1, 4_102_444_800_000_000_000, u64::MAX] {
			let due = DateTime::from_nanos(nanos);
			let encoded = QueueDueKey::encoded(QueueId(1), 2, due, RowNumber(9));
			let decoded = QueueDueKey::decode(&encoded).unwrap();
			assert_eq!(decoded.due.to_nanos(), nanos);
			assert_eq!(decoded.row, RowNumber(9));
			assert_eq!(decoded.partition, 2);
		}
	}

	#[test]
	fn test_partition_scan_excludes_neighbouring_partitions() {
		// The obvious "partition - 1" end bound underflows at partition 0 and would scan the
		// whole queue; this pins the prefix-derived range instead. A scan that leaked into a
		// neighbouring partition would let a claim take work it does not hold the lock for.
		for partition in [0u16, 1, 1023] {
			let range = QueueItemStateKey::partition_scan(QueueId(4), partition);

			assert!(contains(&range, &QueueItemStateKey::encoded(QueueId(4), partition, RowNumber(0))));
			assert!(contains(
				&range,
				&QueueItemStateKey::encoded(QueueId(4), partition, RowNumber(u64::MAX))
			));

			for other in [partition.wrapping_sub(1), partition + 1] {
				if other == partition {
					continue;
				}
				let neighbour = QueueItemStateKey::encoded(QueueId(4), other, RowNumber(1));
				assert!(
					!contains(&range, &neighbour),
					"partition {other} must fall outside {partition}"
				);
			}

			let other_queue = QueueItemStateKey::encoded(QueueId(5), partition, RowNumber(1));
			assert!(!contains(&range, &other_queue), "queue 5 must fall outside queue 4's partition scan");
		}
	}

	#[test]
	fn test_queue_scan_covers_every_partition_of_one_queue_only() {
		// DROP QUEUE wipes the scheduling keyspace through this range: missing a partition
		// leaks records that hydration would later re-admit into a queue that no longer exists.
		let range = QueueDueKey::queue_scan(QueueId(4));

		for partition in [0u16, 1, 1023] {
			let inside = QueueDueKey::encoded(QueueId(4), partition, DateTime::from_nanos(7), RowNumber(1));
			assert!(contains(&range, &inside), "partition {partition} must fall inside the queue scan");
		}

		for queue in [QueueId(3), QueueId(5)] {
			let outside = QueueDueKey::encoded(queue, 0, DateTime::from_nanos(7), RowNumber(1));
			assert!(!contains(&range, &outside), "queue {queue:?} must fall outside queue 4's scan");
		}
	}

	#[test]
	fn test_due_keys_sort_latest_due_first() {
		// Keys are stored bitwise-inverted, so forward iteration yields the LATEST due time
		// first. Step 4's claim must therefore scan in reverse; if this inversion ever
		// changes, claims would silently drain newest-first and starve the oldest work.
		let earlier = QueueDueKey::encoded(QueueId(1), 0, DateTime::from_nanos(1_000), RowNumber(1));
		let later = QueueDueKey::encoded(QueueId(1), 0, DateTime::from_nanos(2_000), RowNumber(1));

		assert!(later.as_slice() < earlier.as_slice(), "the later due time must encode to the smaller key");
	}

	#[test]
	fn test_a_foreign_kind_does_not_decode() {
		// The three families share a prefix layout, so a mis-tagged key would otherwise
		// decode cleanly and address the wrong record entirely.
		let encoded = QueueItemStateKey::encoded(QueueId(1), 0, RowNumber(1));

		assert_eq!(QueuePartitionKey::decode(&encoded), None);
		assert_eq!(QueueDueKey::decode(&encoded), None);
		assert_eq!(QueueItemStateKey::decode(&EncodedKey::new(encoded.as_slice()[..3].to_vec())), None);
	}

	#[test]
	fn test_key_active_key_roundtrips() {
		let encoded = QueueKeyActiveKey::encoded(QueueId(3), 5, 0xDEAD_BEEF_CAFE_F00D, RowNumber(42));
		let decoded = QueueKeyActiveKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(3));
		assert_eq!(decoded.partition, 5);
		assert_eq!(decoded.key_hash, 0xDEAD_BEEF_CAFE_F00D);
		assert_eq!(decoded.row, RowNumber(42));
	}

	#[test]
	fn test_key_active_keys_sort_largest_row_first() {
		let first = QueueKeyActiveKey::encoded(QueueId(1), 0, 77, RowNumber(1));
		let middle = QueueKeyActiveKey::encoded(QueueId(1), 0, 77, RowNumber(5));
		let last = QueueKeyActiveKey::encoded(QueueId(1), 0, 77, RowNumber(9));

		assert!(last.as_slice() < middle.as_slice());
		assert!(middle.as_slice() < first.as_slice());
	}

	#[test]
	fn test_key_scan_excludes_neighbouring_keys_and_partitions() {
		let range = QueueKeyActiveKey::key_scan(QueueId(4), 2, 77);

		assert!(contains(&range, &QueueKeyActiveKey::encoded(QueueId(4), 2, 77, RowNumber(0))));
		assert!(contains(&range, &QueueKeyActiveKey::encoded(QueueId(4), 2, 77, RowNumber(u64::MAX))));

		for other_hash in [76u64, 78, 0, u64::MAX] {
			let neighbour = QueueKeyActiveKey::encoded(QueueId(4), 2, other_hash, RowNumber(1));
			assert!(!contains(&range, &neighbour), "key hash {other_hash} must fall outside key 77");
		}

		let other_partition = QueueKeyActiveKey::encoded(QueueId(4), 3, 77, RowNumber(1));
		assert!(!contains(&range, &other_partition), "partition 3 must fall outside partition 2");

		let other_queue = QueueKeyActiveKey::encoded(QueueId(5), 2, 77, RowNumber(1));
		assert!(!contains(&range, &other_queue), "queue 5 must fall outside queue 4");
	}

	#[test]
	fn test_partition_scan_covers_every_key_of_one_partition_only() {
		let range = QueueKeyActiveKey::partition_scan(QueueId(4), 2);

		for key_hash in [0u64, 77, u64::MAX] {
			let inside = QueueKeyActiveKey::encoded(QueueId(4), 2, key_hash, RowNumber(1));
			assert!(contains(&range, &inside), "key hash {key_hash} must fall inside the partition scan");
		}

		let other_partition = QueueKeyActiveKey::encoded(QueueId(4), 3, 77, RowNumber(1));
		assert!(!contains(&range, &other_partition));
	}
}
