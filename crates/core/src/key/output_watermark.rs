// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;

use super::{
	KeyTag,
	catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};
use crate::{
	interface::catalog::object::ObjectId,
	key::{
		any::{Field, KeyFields, Width},
		bound::TaggedKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = OutputWatermark)]
pub struct OutputWatermarkKey {
	pub object: ObjectId,
}

impl OutputWatermarkKey {
	pub fn new(object: impl Into<ObjectId>) -> Self {
		Self {
			object: object.into(),
		}
	}

	pub fn encoded(object: impl Into<ObjectId>) -> EncodedKey {
		Self::new(object).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::key::encoded::EncodedKey;

	use super::OutputWatermarkKey;
	use crate::{
		interface::catalog::{id::ViewId, object::ObjectId},
		key::KeyTag,
	};

	#[test]
	fn test_encode_decode() {
		let key = OutputWatermarkKey {
			object: ObjectId::View(ViewId(0xABCD)),
		};
		let encoded = key.encode();
		let decoded = OutputWatermarkKey::decode(&encoded).unwrap();

		assert_eq!(decoded.object, ObjectId::View(ViewId(0xABCD)));
		assert_eq!(key, decoded);
	}

	#[test]
	fn the_tag_byte_is_the_inverted_kind() {
		// extend_u8 inverts, so a raw 0x1D here would sort into a neighbouring keyspace.
		let encoded = OutputWatermarkKey::encoded(ObjectId::View(ViewId(1)));

		assert_eq!(encoded.as_slice()[0], !(KeyTag::OutputWatermark as u8));
	}

	#[test]
	fn a_key_of_another_kind_never_decodes_as_an_output_watermark() {
		// 0x1D previously held FlowNodeInternalState, so a stale row must be rejected, never misread.
		let mut foreign = OutputWatermarkKey::encoded(ObjectId::View(ViewId(1))).as_slice().to_vec();
		foreign[0] = !(KeyTag::FlowEdgeByFlow as u8);

		assert!(OutputWatermarkKey::decode(&EncodedKey::new(foreign)).is_none());
	}

	#[test]
	fn every_object_kind_round_trips_under_the_one_output_watermark_tag() {
		// A kind landing on another tag escapes the hydration scan, so its consumer pins at the epoch forever.
		for object in [
			ObjectId::table(1),
			ObjectId::View(ViewId(2)),
			ObjectId::series(3),
			ObjectId::ringbuffer(4),
			ObjectId::queue(5),
		] {
			let encoded = OutputWatermarkKey::encoded(object);

			assert_eq!(
				encoded.as_slice()[0],
				!(KeyTag::OutputWatermark as u8),
				"{:?} encoded under a foreign tag",
				object
			);
			assert_eq!(OutputWatermarkKey::decode(&encoded).unwrap().object, object);
		}
	}

	#[test]
	fn test_order_preserving_within_same_object_kind() {
		let key1 = OutputWatermarkKey {
			object: ObjectId::table(1),
		};
		let key2 = OutputWatermarkKey {
			object: ObjectId::table(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::OutputWatermarkKey;
	use crate::{
		interface::catalog::{id::ViewId, object::ObjectId},
		key::catalog::KeySerializerCatalogExt,
	};

	fn legacy_encode(key: &OutputWatermarkKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(OutputWatermarkKey::TAG as u8).extend_object_id(key.object);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for object in [
			ObjectId::table(1),
			ObjectId::View(ViewId(2)),
			ObjectId::series(3),
			ObjectId::ringbuffer(4),
			ObjectId::queue(5),
		] {
			let key = OutputWatermarkKey {
				object,
			};
			assert_eq!(legacy_encode(&key), key.encode().as_slice().to_vec(), "object={object:?}");
		}
	}
}
