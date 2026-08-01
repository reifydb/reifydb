// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::QueueId;

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeduplicationKey {
	pub queue: QueueId,
	pub key: Vec<u8>,
}

impl QueueDeduplicationKey {
	pub fn new(queue: impl Into<QueueId>, key: impl Into<Vec<u8>>) -> Self {
		Self {
			queue: queue.into(),
			key: key.into(),
		}
	}

	pub fn encoded(queue: impl Into<QueueId>, key: impl Into<Vec<u8>>) -> EncodedKey {
		Self::new(queue, key).encode()
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
		let mut serializer = KeySerializer::with_capacity(9 + self.key.len() + 1);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.queue).extend_bytes(&self.key);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;
		let deduplication = de.read_bytes().ok()?;

		Some(Self {
			queue: QueueId(queue),
			key: deduplication,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_encode_decode_roundtrip() {
		// A lossy codec either resurrects a claimed key or fails to recognise one, and both turn a
		// duplicate enqueue into a second work item.
		let encoded = QueueDeduplicationKey::encoded(QueueId(3), b"invoice-42".to_vec());
		let decoded = QueueDeduplicationKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(3));
		assert_eq!(decoded.key, b"invoice-42".to_vec());
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
			assert_eq!(decoded.key, key, "tail {key:?} must round-trip unchanged");
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
