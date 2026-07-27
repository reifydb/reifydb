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
pub struct QueueIdempotencyKey {
	pub queue: QueueId,
	pub key: Vec<u8>,
}

impl QueueIdempotencyKey {
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

impl EncodableKey for QueueIdempotencyKey {
	const KIND: KeyKind = KeyKind::QueueIdempotency;

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
		let idempotency = de.read_bytes().ok()?;

		Some(Self {
			queue: QueueId(queue),
			key: idempotency,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::*;

	/// The record is the whole dedup mechanism: a lossy codec would either resurrect a key that
	/// was already claimed or fail to recognise one that was, and in both cases a duplicate
	/// enqueue silently becomes a second work item.
	#[test]
	fn test_encode_decode_roundtrip() {
		let encoded = QueueIdempotencyKey::encoded(QueueId(3), b"invoice-42".to_vec());
		let decoded = QueueIdempotencyKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(3));
		assert_eq!(decoded.key, b"invoice-42".to_vec());
	}

	/// The tail is user-supplied text, so it must survive bytes that the key codec treats as
	/// structural (its terminator and escape) as well as embedded nul and multi-byte utf8.
	/// A key mangled here dedups against the wrong record.
	#[test]
	fn test_arbitrary_bytes_survive_the_tail_encoding() {
		for key in [
			vec![],
			vec![0x00],
			vec![0xff],
			vec![0xff, 0x00, 0xff],
			"order/\u{00e9}\u{4e2d}".as_bytes().to_vec(),
		] {
			let encoded = QueueIdempotencyKey::encoded(QueueId(1), key.clone());
			let decoded = QueueIdempotencyKey::decode(&encoded).unwrap();
			assert_eq!(decoded.key, key, "tail {key:?} must round-trip unchanged");
		}
	}

	/// Two queues may legitimately use the same idempotency key, so the queue id has to
	/// discriminate. Without it, enqueueing "invoice-1" on one queue would suppress the same
	/// logical key on every other queue.
	#[test]
	fn test_the_same_key_in_two_queues_encodes_differently() {
		let a = QueueIdempotencyKey::encoded(QueueId(1), b"same".to_vec());
		let b = QueueIdempotencyKey::encoded(QueueId(2), b"same".to_vec());
		assert_ne!(a, b);
	}

	/// Retention sweeps a queue's records by prefix range. Keys are stored bitwise-inverted, so a
	/// bound derived with the wrong sign would make the sweep either miss its own records or
	/// delete a neighbouring queue's, which would let that queue's duplicates through.
	#[test]
	fn test_full_scan_contains_only_the_target_queue() {
		let range = QueueIdempotencyKey::full_scan(QueueId(3));
		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		assert!(start.as_slice() < end.as_slice(), "the range must be non-empty under byte order");

		for key in [vec![], b"a".to_vec(), vec![0xff; 64]] {
			let inside = QueueIdempotencyKey::encoded(QueueId(3), key.clone());
			assert!(
				inside.as_slice() >= start.as_slice() && inside.as_slice() <= end.as_slice(),
				"key {key:?} in queue 3 must fall inside the scan range"
			);
		}

		for queue in [QueueId(2), QueueId(4)] {
			let neighbour = QueueIdempotencyKey::encoded(queue, b"a".to_vec());
			assert!(
				neighbour.as_slice() < start.as_slice() || neighbour.as_slice() > end.as_slice(),
				"queue {queue:?} must fall outside queue 3's scan range"
			);
		}
	}

	/// A truncated or mistyped key must fail to decode rather than yield a partial record: a
	/// silent `None` tail would collapse every key in the queue onto one dedup slot.
	#[test]
	fn test_a_foreign_or_truncated_key_does_not_decode() {
		let encoded = QueueIdempotencyKey::encoded(QueueId(3), b"invoice-42".to_vec());

		let mut wrong_kind = encoded.as_slice().to_vec();
		wrong_kind[0] = KeyKind::Queue as u8;
		assert_eq!(QueueIdempotencyKey::decode(&EncodedKey::new(wrong_kind)), None);

		let truncated = encoded.as_slice()[..5].to_vec();
		assert_eq!(QueueIdempotencyKey::decode(&EncodedKey::new(truncated)), None);
	}
}
