// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::object::ObjectId,
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct OutputFrontierKey {
	pub object: ObjectId,
}

impl EncodableKey for OutputFrontierKey {
	const KIND: KeyKind = KeyKind::OutputFrontier;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;

		Some(Self {
			object,
		})
	}
}

impl OutputFrontierKey {
	pub fn encoded(object: impl Into<ObjectId>) -> EncodedKey {
		Self {
			object: object.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::frontier_start()), Some(Self::frontier_end()))
	}

	fn frontier_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn frontier_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::key::encoded::EncodedKey;

	use super::{EncodableKey, OutputFrontierKey};
	use crate::{
		interface::catalog::{id::ViewId, object::ObjectId},
		key::KeyKind,
	};

	#[test]
	fn test_encode_decode() {
		let key = OutputFrontierKey {
			object: ObjectId::View(ViewId(0xABCD)),
		};
		let encoded = key.encode();
		let decoded = OutputFrontierKey::decode(&encoded).unwrap();

		assert_eq!(decoded.object, ObjectId::View(ViewId(0xABCD)));
		assert_eq!(key, decoded);
	}

	#[test]
	fn the_tag_byte_is_the_inverted_kind() {
		// extend_u8 inverts, so a raw 0x1D here would sort into a neighbouring keyspace.
		let encoded = OutputFrontierKey::encoded(ObjectId::View(ViewId(1)));

		assert_eq!(encoded.as_slice()[0], !(KeyKind::OutputFrontier as u8));
	}

	#[test]
	fn a_key_of_another_kind_never_decodes_as_a_frontier() {
		// 0x1D previously held FlowNodeInternalState, so a stale row must be rejected, never misread.
		let mut foreign = OutputFrontierKey::encoded(ObjectId::View(ViewId(1))).as_slice().to_vec();
		foreign[0] = !(KeyKind::FlowEdgeByFlow as u8);

		assert!(OutputFrontierKey::decode(&EncodedKey::new(foreign)).is_none());
	}

	#[test]
	fn every_object_kind_round_trips_under_the_one_frontier_tag() {
		// A kind landing on another tag escapes the hydration scan, so its consumer pins at the epoch forever.
		for object in [
			ObjectId::table(1),
			ObjectId::View(ViewId(2)),
			ObjectId::series(3),
			ObjectId::ringbuffer(4),
			ObjectId::queue(5),
		] {
			let encoded = OutputFrontierKey::encoded(object);

			assert_eq!(
				encoded.as_slice()[0],
				!(KeyKind::OutputFrontier as u8),
				"{:?} encoded under a foreign tag",
				object
			);
			assert_eq!(OutputFrontierKey::decode(&encoded).unwrap().object, object);
		}
	}
}
