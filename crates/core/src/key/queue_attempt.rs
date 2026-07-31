// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::row_number::RowNumber;

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::QueueId;

#[derive(Debug, Clone, PartialEq)]
pub struct QueueAttemptKey {
	pub queue: QueueId,
	pub row: RowNumber,
	pub attempt: u32,
}

impl EncodableKey for QueueAttemptKey {
	const KIND: KeyKind = KeyKind::QueueAttempt;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(21);
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.queue)
			.extend_u64(self.row.0)
			.extend_u32(self.attempt);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let row = de.read_row_number().ok()?;
		let attempt = de.read_u32().ok()?;

		Some(Self {
			queue: QueueId(queue),
			row,
			attempt,
		})
	}
}

impl QueueAttemptKey {
	pub fn encoded(queue: impl Into<QueueId>, row: impl Into<RowNumber>, attempt: u32) -> EncodedKey {
		Self {
			queue: queue.into(),
			row: row.into(),
			attempt,
		}
		.encode()
	}

	pub fn item_scan(queue: QueueId, row: RowNumber) -> EncodedKeyRange {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(Self::KIND as u8).extend_u64(queue).extend_u64(row.0);
		EncodedKeyRange::prefix(serializer.to_encoded_key().as_slice())
	}

	pub fn queue_scan(queue: QueueId) -> EncodedKeyRange {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(queue);
		EncodedKeyRange::prefix(serializer.to_encoded_key().as_slice())
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
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
	fn test_attempt_key_roundtrips() {
		// Attempt is the CAS discriminator the whole ack path turns on: a lost or widened
		// attempt component would let attempt 2's record answer for attempt 1, which is
		// exactly the "first outcome wins" guarantee acks rely on.
		let key = QueueAttemptKey {
			queue: QueueId(7),
			row: RowNumber(42),
			attempt: u32::MAX,
		};

		assert_eq!(QueueAttemptKey::decode(&key.encode()), Some(key));
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

		assert_eq!(QueueAttemptKey::decode(&key.encode()), Some(key));
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
		let foreign = super::super::queue_schedule::QueueItemStateKey::encoded(QueueId(1), 0, RowNumber(1));

		assert_eq!(QueueAttemptKey::decode(&foreign), None);
	}
}
