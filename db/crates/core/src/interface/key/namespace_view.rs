// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{NamespaceId, ViewId},
	util::encoding::keycode::{self, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceViewKey {
	pub namespace: NamespaceId,
	pub view: ViewId,
}

const VERSION: u8 = 1;

impl EncodableKey for NamespaceViewKey {
	const KIND: KeyKind = KeyKind::NamespaceView;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.namespace)
			.extend_u64(self.view);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let payload = &key[2..];
		if payload.len() != 16 {
			return None;
		}

		keycode::deserialize(&payload[..8]).ok().zip(keycode::deserialize(&payload[8..]).ok()).map(
			|(namespace, view)| Self {
				namespace,
				view,
			},
		)
	}
}

impl NamespaceViewKey {
	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, NamespaceViewKey};
	use crate::interface::catalog::{NamespaceId, ViewId};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceViewKey {
			namespace: NamespaceId(0xABCD),
			view: ViewId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xEE, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceViewKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.view, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceViewKey {
			namespace: NamespaceId(1),
			view: ViewId(100),
		};
		let key2 = NamespaceViewKey {
			namespace: NamespaceId(1),
			view: ViewId(200),
		};
		let key3 = NamespaceViewKey {
			namespace: NamespaceId(2),
			view: ViewId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
