// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use super::{EncodableKey, KeyKind};
use crate::{interface::catalog::id::QueueId, key::typed::key::Key};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Queue)]
pub struct QueueKey {
	pub queue: QueueId,
}

impl QueueKey {
	pub fn new(queue: QueueId) -> Self {
		Self {
			queue,
		}
	}

	pub fn encoded(queue: impl Into<QueueId>) -> EncodedKey {
		Key::encode(&Self::new(queue.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::queue_start()), Some(Self::queue_end()))
	}

	fn queue_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<QueueKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn queue_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<QueueKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod queue_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_encode_decode_roundtrip() {
		// A queue def row is addressed by this key alone, so a broken codec orphans every definition.
		let encoded = QueueKey::encoded(QueueId(42));
		let decoded = <QueueKey as Key>::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(42));
	}

	#[test]
	fn test_decode_rejects_foreign_kind() {
		// The kind byte guards the family: a foreign key must fail rather than have its payload
		// reinterpreted as a queue id.
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyKind::NamespaceQueue as u8).extend_u64(7u64);
		assert!(<QueueKey as Key>::decode(&serializer.to_encoded_key()).is_none());
	}

	#[test]
	fn test_full_scan_brackets_every_queue_key() {
		// Keys are stored bitwise-inverted, so byte order runs opposite to the logical value; that is
		// why the range ends at KIND - 1. Reversing the bound makes list_queues return nothing.
		let range = QueueKey::full_scan();

		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		assert_eq!(start.as_slice(), &[!(KeyKind::Queue as u8)]);
		assert_eq!(end.as_slice(), &[!(KeyKind::Queue as u8 - 1)]);
		assert!(start.as_slice() < end.as_slice(), "the range must be non-empty under byte order");

		for id in [QueueId(1), QueueId(u64::MAX)] {
			let key = QueueKey::encoded(id);
			assert!(
				key.as_slice() >= start.as_slice() && key.as_slice() <= end.as_slice(),
				"queue {id:?} must fall inside the scan range"
			);
		}
	}

	#[test]
	fn test_full_scan_excludes_the_neighbouring_kind() {
		// A neighbouring key family inside the range would let a full scan decode foreign rows as
		// queue definitions.
		let range = QueueKey::full_scan();
		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyKind::NamespaceQueue as u8).extend_u64(1u64);
		let foreign = serializer.to_encoded_key();

		assert!(
			foreign.as_slice() < start.as_slice() || foreign.as_slice() > end.as_slice(),
			"a NamespaceQueue key must fall outside the QueueKey scan range"
		);
	}
}

#[cfg(test)]
mod byte_identical_check_queue_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &QueueKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyKind::Queue as u8).extend_u64(key.queue);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for id in [QueueId(0), QueueId(1), QueueId(u64::MAX)] {
			let key = QueueKey {
				queue: id,
			};
			assert_eq!(legacy_encode(&key).as_slice(), Key::encode(&key).as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = QueueAttempt)]
pub struct QueueAttemptKey {
	pub queue: QueueId,
	pub row: RowNumber,
	pub attempt: u32,
}

impl QueueAttemptKey {
	pub fn encoded(queue: impl Into<QueueId>, row: impl Into<RowNumber>, attempt: u32) -> EncodedKey {
		Key::encode(&Self {
			queue: queue.into(),
			row: row.into(),
			attempt,
		})
	}

	pub fn item_scan(queue: QueueId, row: RowNumber) -> EncodedKeyRange {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(<QueueAttemptKey as Key>::KIND as u8).extend_u64(queue).extend_u64(row.0);
		EncodedKeyRange::prefix(serializer.to_encoded_key().as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<QueueAttemptKey as Key>::KIND as u8).extend_u64(queue);
		EncodedKeyRange::prefix(serializer.to_encoded_key().as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(<QueueAttemptKey as Key>::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(<QueueAttemptKey as Key>::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

#[cfg(test)]
mod queue_item_state_key_tests {
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
	fn test_attempt_key_roundtrips() {
		// Attempt is the CAS discriminator the whole ack path turns on: a lost or widened
		// attempt component would let attempt 2's record answer for attempt 1, which is
		// exactly the "first outcome wins" guarantee acks rely on.
		let key = QueueAttemptKey {
			queue: QueueId(7),
			row: RowNumber(42),
			attempt: u32::MAX,
		};

		assert_eq!(<QueueAttemptKey as Key>::decode(&Key::encode(&key)), Some(key));
	}

	#[test]
	fn test_attempt_zero_roundtrips() {
		// Attempt 0 never reaches storage today (claim leases at attempt 1), but the codec
		// must not treat it as an absent component; step 5's reaper writes lost attempts and
		// a zero-eliding encoding would collide with the item's own prefix.
		let key = QueueAttemptKey {
			queue: QueueId(0),
			row: RowNumber(0),
			attempt: 0,
		};

		assert_eq!(<QueueAttemptKey as Key>::decode(&Key::encode(&key)), Some(key));
	}

	#[test]
	fn test_item_scan_excludes_neighbouring_items_and_queues() {
		// Retention and repeat-detection both enumerate one item's attempts. If the scan
		// leaked into the adjacent row, acking item 5 would observe item 6's history and
		// report a repeat for work that was never done.
		let range = QueueAttemptKey::item_scan(QueueId(3), RowNumber(5));

		assert!(contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(5), 0)));
		assert!(contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(5), u32::MAX)));
		assert!(!contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(6), 0)));
		assert!(!contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(4), 0)));
		assert!(!contains(&range, &QueueAttemptKey::encoded(QueueId(4), RowNumber(5), 0)));
	}

	#[test]
	fn test_queue_scan_covers_every_item_of_one_queue_only() {
		// DROP QUEUE teardown and step-5 retention both sweep by queue; a range that missed
		// row 0 or spilled into the next queue would either leak audit rows forever or delete
		// another queue's history.
		let range = QueueAttemptKey::queue_scan(QueueId(3));

		assert!(contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(0), 0)));
		assert!(contains(&range, &QueueAttemptKey::encoded(QueueId(3), RowNumber(u64::MAX), 9)));
		assert!(!contains(&range, &QueueAttemptKey::encoded(QueueId(2), RowNumber(1), 0)));
		assert!(!contains(&range, &QueueAttemptKey::encoded(QueueId(4), RowNumber(1), 0)));
	}

	#[test]
	fn test_a_foreign_kind_does_not_decode() {
		// Every family shares the single-lane and MVCC keyspace; decoding a neighbour's key
		// as an attempt record would attribute another object's bytes to a queue item.
		let foreign = QueueItemStateKey::encoded(QueueId(1), 0, RowNumber(1));

		assert_eq!(<QueueAttemptKey as Key>::decode(&foreign), None);
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeduplicationKey {
	pub queue: QueueId,
	pub tail: EncodedKey,
}

impl QueueDeduplicationKey {
	pub fn new(queue: impl Into<QueueId>, tail: impl AsRef<[u8]>) -> Self {
		Self {
			queue: queue.into(),
			tail: EncodedKey::new(tail),
		}
	}

	pub fn encoded(queue: impl Into<QueueId>, tail: impl AsRef<[u8]>) -> EncodedKey {
		Self::new(queue, tail).encode()
	}

	pub fn full_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::scan_start(queue)), Some(Self::scan_end(queue)))
	}

	fn scan_start(queue: QueueId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(queue);
		serializer.to_encoded_key()
	}

	fn scan_end(queue: QueueId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*queue - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for QueueDeduplicationKey {
	const KIND: KeyKind = KeyKind::QueueDeduplication;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9 + self.tail.len() + 1);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.queue).extend_bytes(&self.tail);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let tail = de.read_bytes().ok()?;

		Some(Self {
			queue: QueueId(queue),
			tail: EncodedKey::new(tail),
		})
	}
}

#[cfg(test)]
mod queue_deduplication_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_encode_decode_roundtrip() {
		// A lossy codec either resurrects a claimed key or fails to recognise one, and both turn a
		// duplicate enqueue into a second work item.
		let encoded = QueueDeduplicationKey::encoded(QueueId(3), b"invoice-42".to_vec());
		let decoded = QueueDeduplicationKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(3));
		assert_eq!(decoded.tail.as_slice(), b"invoice-42");
	}

	#[test]
	fn test_arbitrary_bytes_survive_the_tail_encoding() {
		// The tail is user-supplied, so it must survive the bytes the key codec treats as structural
		// as well as embedded nul and multi-byte utf8; a mangled key dedups against the wrong record.
		for key in [
			vec![],
			vec![0x00],
			vec![0xff],
			vec![0xff, 0x00, 0xff],
			"order/\u{00e9}\u{4e2d}".as_bytes().to_vec(),
		] {
			let encoded = QueueDeduplicationKey::encoded(QueueId(1), key.clone());
			let decoded = QueueDeduplicationKey::decode(&encoded).unwrap();
			assert_eq!(decoded.tail.as_slice(), key.as_slice(), "tail {key:?} must round-trip unchanged");
		}
	}

	#[test]
	fn test_the_same_key_in_two_queues_encodes_differently() {
		// Two queues may legitimately use the same dedup key, so without the queue id discriminating,
		// enqueueing "invoice-1" on one queue would suppress it on every other queue.
		let a = QueueDeduplicationKey::encoded(QueueId(1), b"same".to_vec());
		let b = QueueDeduplicationKey::encoded(QueueId(2), b"same".to_vec());
		assert_ne!(a, b);
	}

	#[test]
	fn test_full_scan_contains_only_the_target_queue() {
		// Keys are stored bitwise-inverted, so a bound derived with the wrong sign makes the retention
		// sweep either miss its own records or delete a neighbouring queue's.
		let range = QueueDeduplicationKey::full_scan(QueueId(3));
		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		assert!(start.as_slice() < end.as_slice(), "the range must be non-empty under byte order");

		for key in [vec![], b"a".to_vec(), vec![0xff; 64]] {
			let inside = QueueDeduplicationKey::encoded(QueueId(3), key.clone());
			assert!(
				inside.as_slice() >= start.as_slice() && inside.as_slice() <= end.as_slice(),
				"key {key:?} in queue 3 must fall inside the scan range"
			);
		}

		for queue in [QueueId(2), QueueId(4)] {
			let neighbour = QueueDeduplicationKey::encoded(queue, b"a".to_vec());
			assert!(
				neighbour.as_slice() < start.as_slice() || neighbour.as_slice() > end.as_slice(),
				"queue {queue:?} must fall outside queue 3's scan range"
			);
		}
	}

	#[test]
	fn test_a_foreign_or_truncated_key_does_not_decode() {
		// A partial record would collapse every key in the queue onto one dedup slot.
		let encoded = QueueDeduplicationKey::encoded(QueueId(3), b"invoice-42".to_vec());

		let mut wrong_kind = encoded.as_slice().to_vec();
		wrong_kind[0] = KeyKind::Queue as u8;
		assert_eq!(QueueDeduplicationKey::decode(&EncodedKey::new(wrong_kind)), None);

		let truncated = encoded.as_slice()[..5].to_vec();
		assert_eq!(QueueDeduplicationKey::decode(&EncodedKey::new(truncated)), None);
	}
}

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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = QueuePartition)]
pub struct QueuePartitionKey {
	pub queue: QueueId,
	pub partition: u16,
}

impl QueuePartitionKey {
	pub fn encoded(queue: impl Into<QueueId>, partition: u16) -> EncodedKey {
		Key::encode(&Self {
			queue: queue.into(),
			partition,
		})
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(<QueuePartitionKey as Key>::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(<QueuePartitionKey as Key>::KIND)
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = QueueItemState)]
pub struct QueueItemStateKey {
	pub queue: QueueId,
	pub partition: u16,
	pub row: RowNumber,
}

impl QueueItemStateKey {
	pub fn encoded(queue: impl Into<QueueId>, partition: u16, row: impl Into<RowNumber>) -> EncodedKey {
		Key::encode(&Self {
			queue: queue.into(),
			partition,
			row: row.into(),
		})
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(<QueueItemStateKey as Key>::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(<QueueItemStateKey as Key>::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(<QueueItemStateKey as Key>::KIND)
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = QueueDue)]
pub struct QueueDueKey {
	pub queue: QueueId,
	pub partition: u16,
	pub due: DateTime,
	pub row: RowNumber,
}

impl QueueDueKey {
	pub fn encoded(
		queue: impl Into<QueueId>,
		partition: u16,
		due: DateTime,
		row: impl Into<RowNumber>,
	) -> EncodedKey {
		Key::encode(&Self {
			queue: queue.into(),
			partition,
			due,
			row: row.into(),
		})
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(<QueueDueKey as Key>::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(<QueueDueKey as Key>::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(<QueueDueKey as Key>::KIND)
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = QueueKeyActive)]
pub struct QueueKeyActiveKey {
	pub queue: QueueId,
	pub partition: u16,
	pub key_hash: u64,
	pub row: RowNumber,
}

impl QueueKeyActiveKey {
	pub fn encoded(
		queue: impl Into<QueueId>,
		partition: u16,
		key_hash: u64,
		row: impl Into<RowNumber>,
	) -> EncodedKey {
		Key::encode(&Self {
			queue: queue.into(),
			partition,
			key_hash,
			row: row.into(),
		})
	}

	pub fn key_scan(queue: QueueId, partition: u16, key_hash: u64) -> EncodedKeyRange {
		EncodedKeyRange::prefix(
			key_prefix(<QueueKeyActiveKey as Key>::KIND, queue, partition, key_hash).as_slice(),
		)
	}

	pub fn partition_scan(queue: QueueId, partition: u16) -> EncodedKeyRange {
		EncodedKeyRange::prefix(partition_prefix(<QueueKeyActiveKey as Key>::KIND, queue, partition).as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		EncodedKeyRange::prefix(queue_prefix(<QueueKeyActiveKey as Key>::KIND, queue).as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		family_scan(<QueueKeyActiveKey as Key>::KIND)
	}
}

#[cfg(test)]
mod queue_partition_key_tests {
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
			let decoded = <QueuePartitionKey as Key>::decode(&encoded).unwrap();
			assert_eq!(decoded.queue, QueueId(7));
			assert_eq!(decoded.partition, partition);
		}
	}

	#[test]
	fn test_item_state_key_roundtrips() {
		// The state record is the compare-and-set target for every transition; a key that
		// decodes to the wrong row would transition somebody else's item.
		let encoded = QueueItemStateKey::encoded(QueueId(3), 5, RowNumber(42));
		let decoded = <QueueItemStateKey as Key>::decode(&encoded).unwrap();
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
			let decoded = <QueueDueKey as Key>::decode(&encoded).unwrap();
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

		assert_eq!(<QueuePartitionKey as Key>::decode(&encoded), None);
		assert_eq!(<QueueDueKey as Key>::decode(&encoded), None);
		assert_eq!(
			<QueueKeyActiveKey as Key>::decode(&EncodedKey::new(encoded.as_slice()[..3].to_vec())),
			None
		);
	}

	#[test]
	fn test_key_active_key_roundtrips() {
		let encoded = QueueKeyActiveKey::encoded(QueueId(3), 5, 0xDEAD_BEEF_CAFE_F00D, RowNumber(42));
		let decoded = <QueueKeyActiveKey as Key>::decode(&encoded).unwrap();
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

	#[test]
	fn test_schedule_keys_match_legacy_byte_layout() {
		let queue = QueueId(7);
		let partition = 3u16;
		let row = RowNumber(42);

		let mut legacy = KeySerializer::with_capacity(11);
		legacy.extend_u8(KeyKind::QueuePartition as u8).extend_u64(queue).extend_u16(partition);
		assert_eq!(legacy.to_encoded_key().as_slice(), QueuePartitionKey::encoded(queue, partition).as_slice());

		let mut legacy = KeySerializer::with_capacity(19);
		legacy.extend_u8(KeyKind::QueueItemState as u8)
			.extend_u64(queue)
			.extend_u16(partition)
			.extend_u64(row.0);
		assert_eq!(
			legacy.to_encoded_key().as_slice(),
			QueueItemStateKey::encoded(queue, partition, row).as_slice()
		);

		let due = DateTime::from_nanos(1_000);
		let mut legacy = KeySerializer::with_capacity(27);
		legacy.extend_u8(KeyKind::QueueDue as u8)
			.extend_u64(queue)
			.extend_u16(partition)
			.extend_datetime(&due)
			.extend_u64(row.0);
		assert_eq!(
			legacy.to_encoded_key().as_slice(),
			QueueDueKey::encoded(queue, partition, due, row).as_slice()
		);

		let key_hash = 0xDEAD_BEEFu64;
		let mut legacy = KeySerializer::with_capacity(28);
		legacy.extend_u8(KeyKind::QueueKeyActive as u8)
			.extend_u64(queue)
			.extend_u16(partition)
			.extend_u64(key_hash)
			.extend_u64(row.0);
		assert_eq!(
			legacy.to_encoded_key().as_slice(),
			QueueKeyActiveKey::encoded(queue, partition, key_hash, row).as_slice()
		);
	}
}
