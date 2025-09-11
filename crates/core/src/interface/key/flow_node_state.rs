// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange, interface::FlowNodeId,
	util::encoding::keycode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNodeStateKey {
	pub node: FlowNodeId,
}

const VERSION: u8 = 1;

impl EncodableKey for FlowNodeStateKey {
	const KIND: KeyKind = KeyKind::FlowNodeState;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.node.0));
		EncodedKey::new(out)
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
		if payload.len() != 8 {
			return None;
		}

		keycode::deserialize(&payload[..8]).ok().map(|id| Self {
			node: FlowNodeId(id),
		})
	}
}

impl FlowNodeStateKey {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::operator_state_start()),
			Some(Self::operator_state_end()),
		)
	}

	fn operator_state_start() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&Self::KIND));
		EncodedKey::new(out)
	}

	fn operator_state_end() -> EncodedKey {
		let mut out = Vec::with_capacity(2);
		out.extend(&keycode::serialize(&VERSION));
		out.extend(&keycode::serialize(&(Self::KIND as u8 + 1)));
		EncodedKey::new(out)
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, FlowNodeStateKey};
	use crate::EncodedKey;

	#[test]
	fn test_encode_decode() {
		let key = FlowNodeStateKey {
			node: crate::interface::FlowNodeId(0xDEADBEEF),
		};
		let encoded = key.encode();

		// Verify the encoded format
		assert_eq!(encoded[0], 0xFE); // version
		assert_eq!(encoded[1], 0xEC); // kind (0x13 encoded)

		let decoded = FlowNodeStateKey::decode(&encoded).unwrap();
		assert_eq!(decoded.node.0, 0xDEADBEEF);
	}

	#[test]
	fn test_new() {
		let key =
			FlowNodeStateKey::new(crate::interface::FlowNodeId(42));
		assert_eq!(key.node.0, 42);
	}

	#[test]
	fn test_roundtrip() {
		let original = FlowNodeStateKey {
			node: crate::interface::FlowNodeId(999_999_999),
		};
		let encoded = original.encode();
		let decoded = FlowNodeStateKey::decode(&encoded).unwrap();
		assert_eq!(original, decoded);
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = Vec::new();
		encoded.push(0xFF); // wrong version
		encoded.push(0xEC); // correct kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&999u64.to_be_bytes());
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let mut encoded = Vec::new();
		encoded.push(0xFE); // correct version
		encoded.push(0xEC); // correct kind
		encoded.extend(&999u32.to_be_bytes()); // wrong size
		let key = EncodedKey::new(encoded);
		assert!(FlowNodeStateKey::decode(&key).is_none());
	}
}
