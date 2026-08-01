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
		Self::new(queue.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::queue_start()), Some(Self::queue_end()))
	}

	fn queue_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn queue_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for QueueKey {
	const KIND: KeyKind = KeyKind::Queue;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.queue);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let queue = de.read_u64().ok()?;

		Some(Self {
			queue: QueueId(queue),
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_encode_decode_roundtrip() {
		// A queue def row is addressed by this key alone, so a broken codec orphans every definition.
		let encoded = QueueKey::encoded(QueueId(42));
		let decoded = QueueKey::decode(&encoded).unwrap();
		assert_eq!(decoded.queue, QueueId(42));
	}

	#[test]
	fn test_decode_rejects_foreign_kind() {
		// The kind byte guards the family: a foreign key must fail rather than have its payload
		// reinterpreted as a queue id.
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyKind::NamespaceQueue as u8).extend_u64(7u64);
		assert!(QueueKey::decode(&serializer.to_encoded_key()).is_none());
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
