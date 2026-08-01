// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::{NamespaceId, QueueId};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceQueueKey {
	pub namespace: NamespaceId,
	pub queue: QueueId,
}

impl NamespaceQueueKey {
	pub fn new(namespace: NamespaceId, queue: QueueId) -> Self {
		Self {
			namespace,
			queue,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, queue: impl Into<QueueId>) -> EncodedKey {
		Self::new(namespace.into(), queue.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for NamespaceQueueKey {
	const KIND: KeyKind = KeyKind::NamespaceQueue;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.namespace).extend_u64(self.queue);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let namespace = de.read_u64().ok()?;
		let queue = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
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
		// The link row is what makes a queue findable by name; losing either component makes DROP
		// NAMESPACE miss its queues.
		let encoded = NamespaceQueueKey::encoded(NamespaceId(3), QueueId(42));
		let decoded = NamespaceQueueKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(3));
		assert_eq!(decoded.queue, QueueId(42));
	}

	#[test]
	fn test_full_scan_contains_only_the_target_namespace() {
		// Keys are stored bitwise-inverted, so a bound derived with the wrong sign would make DROP
		// NAMESPACE either miss its queues or reach into a sibling.
		let range = NamespaceQueueKey::full_scan(NamespaceId(3));
		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		assert!(start.as_slice() < end.as_slice(), "the range must be non-empty under byte order");

		for queue in [QueueId(1), QueueId(u64::MAX)] {
			let inside = NamespaceQueueKey::encoded(NamespaceId(3), queue);
			assert!(
				inside.as_slice() >= start.as_slice() && inside.as_slice() <= end.as_slice(),
				"queue {queue:?} in namespace 3 must fall inside the scan range"
			);
		}

		for namespace in [NamespaceId(2), NamespaceId(4)] {
			let neighbour = NamespaceQueueKey::encoded(namespace, QueueId(1));
			assert!(
				neighbour.as_slice() < start.as_slice() || neighbour.as_slice() > end.as_slice(),
				"namespace {namespace:?} must fall outside namespace 3's scan range"
			);
		}
	}
}
