// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::id::SubscriptionId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	value::encoded::key::{EncodedKey, EncodedKeyRange},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionKey {
	pub subscription: SubscriptionId,
}

const VERSION: u8 = 1;

impl EncodableKey for SubscriptionKey {
	const KIND: KeyKind = KeyKind::Subscription;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 16 bytes for UUID
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_bytes(self.subscription.as_bytes());
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

		// read_bytes handles the escaped encoding used by extend_bytes
		let bytes = de.read_bytes().ok()?;
		let uuid_bytes: [u8; 16] = bytes.try_into().ok()?;

		Some(Self {
			subscription: SubscriptionId::from_bytes(uuid_bytes),
		})
	}
}

impl SubscriptionKey {
	pub fn encoded(subscription: impl Into<SubscriptionId>) -> EncodedKey {
		Self {
			subscription: subscription.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::subscription_start()), Some(Self::subscription_end()))
	}

	fn subscription_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn subscription_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8((Self::KIND as u8).wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, SubscriptionKey};
	use crate::interface::catalog::id::SubscriptionId;

	#[test]
	fn test_encode_decode() {
		let subscription_id = SubscriptionId::new();
		let key = SubscriptionKey {
			subscription: subscription_id,
		};
		let encoded = key.encode();

		let decoded = SubscriptionKey::decode(&encoded).unwrap();
		assert_eq!(decoded.subscription, subscription_id);
	}
}
