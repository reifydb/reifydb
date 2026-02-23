// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{HandlerId, NamespaceId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for looking up handlers by the variant they handle.
/// Supports range scans for DISPATCH to find all handlers for a given (namespace, sumtype, variant_tag).
#[derive(Debug, Clone, PartialEq)]
pub struct VariantHandlerKey {
	pub namespace: NamespaceId,
	pub sumtype: SumTypeId,
	pub variant_tag: u8,
	pub handler: HandlerId,
}

impl VariantHandlerKey {
	pub fn new(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8, handler: HandlerId) -> Self {
		Self {
			namespace,
			sumtype,
			variant_tag,
			handler,
		}
	}

	pub fn encoded(
		namespace: impl Into<NamespaceId>,
		sumtype: impl Into<SumTypeId>,
		variant_tag: u8,
		handler: impl Into<HandlerId>,
	) -> EncodedKey {
		Self::new(namespace.into(), sumtype.into(), variant_tag, handler.into()).encode()
	}

	/// Range scan for all handlers of a specific variant.
	pub fn variant_scan(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::variant_start(namespace, sumtype, variant_tag)),
			Some(Self::variant_end(namespace, sumtype, variant_tag)),
		)
	}

	fn variant_start(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(namespace)
			.extend_u64(sumtype)
			.extend_u8(variant_tag);
		serializer.to_encoded_key()
	}

	fn variant_end(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(namespace)
			.extend_u64(sumtype)
			.extend_u8(variant_tag.wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

impl EncodableKey for VariantHandlerKey {
	const KIND: KeyKind = KeyKind::VariantHandler;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(27);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.sumtype)
			.extend_u8(self.variant_tag)
			.extend_u64(self.handler);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let namespace = de.read_u64().ok()?;
		let sumtype = de.read_u64().ok()?;
		let variant_tag = de.read_u8().ok()?;
		let handler = de.read_u64().ok()?;

		Some(Self {
			namespace: NamespaceId(namespace),
			sumtype: SumTypeId(sumtype),
			variant_tag,
			handler: HandlerId(handler),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound;

	use reifydb_type::value::sumtype::SumTypeId;

	use super::{EncodableKey, VariantHandlerKey};
	use crate::interface::catalog::id::{HandlerId, NamespaceId};

	#[test]
	fn test_encode_decode() {
		let key = VariantHandlerKey {
			namespace: NamespaceId(0xABCD),
			sumtype: SumTypeId(0x1234),
			variant_tag: 5,
			handler: HandlerId(0x6789),
		};
		let encoded = key.encode();
		let expected: Vec<u8> = vec![
			0xFE, // version
			0xCF, // kind (VariantHandler = 0x30, encoded as 0xFF ^ 0x30)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, // namespace 0xABCD
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xED, 0xCB, // sumtype 0x1234
			0xFA, // variant_tag 5
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x76, // handler 0x6789
		];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = VariantHandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.sumtype, SumTypeId(0x1234));
		assert_eq!(decoded.variant_tag, 5);
		assert_eq!(decoded.handler, HandlerId(0x6789));
	}

	#[test]
	fn test_order_preserving() {
		let key1 = VariantHandlerKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(5),
			variant_tag: 3,
			handler: HandlerId(100),
		};
		let key2 = VariantHandlerKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(5),
			variant_tag: 3,
			handler: HandlerId(200),
		};
		let key3 = VariantHandlerKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(5),
			variant_tag: 4,
			handler: HandlerId(1),
		};
		let key4 = VariantHandlerKey {
			namespace: NamespaceId(2),
			sumtype: SumTypeId(1),
			variant_tag: 0,
			handler: HandlerId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();
		let encoded4 = key4.encode();

		assert!(encoded4 < encoded3, "ordering not preserved");
		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}

	#[test]
	fn test_variant_scan() {
		let ns = NamespaceId(1);
		let st = SumTypeId(10);
		let tag = 5u8;

		let range = VariantHandlerKey::variant_scan(ns, st, tag);
		let start = match &range.start {
			Bound::Included(k) | Bound::Excluded(k) => k,
			Bound::Unbounded => panic!("expected bounded start"),
		};
		let end = match &range.end {
			Bound::Included(k) | Bound::Excluded(k) => k,
			Bound::Unbounded => panic!("expected bounded end"),
		};

		// A key for this exact variant should fall within the range
		let key = VariantHandlerKey {
			namespace: ns,
			sumtype: st,
			variant_tag: tag,
			handler: HandlerId(42),
		};
		let encoded = key.encode();
		assert!(encoded.as_slice() >= start.as_slice());
		assert!(encoded.as_slice() <= end.as_slice());

		// A key for a higher variant_tag encodes to smaller bytes and is outside the range
		let other = VariantHandlerKey {
			namespace: ns,
			sumtype: st,
			variant_tag: tag + 1,
			handler: HandlerId(42),
		};
		let other_encoded = other.encode();
		assert!(other_encoded.as_slice() < start.as_slice());
	}
}
