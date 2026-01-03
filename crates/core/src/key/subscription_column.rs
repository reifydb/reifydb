// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::catalog::{SubscriptionColumnId, SubscriptionId},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionColumnKey {
	pub subscription: SubscriptionId,
	pub column: SubscriptionColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for SubscriptionColumnKey {
	const KIND: KeyKind = KeyKind::SubscriptionColumn;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26); // 1 + 1 + 16 (UUID) + 8 (column)
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(self.subscription.as_bytes())
			.extend_u64(self.column);
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
		let column = de.read_u64().ok()?;

		Some(Self {
			subscription: SubscriptionId::from_bytes(uuid_bytes),
			column: SubscriptionColumnId(column),
		})
	}
}

impl SubscriptionColumnKey {
	pub fn encoded(subscription: impl Into<SubscriptionId>, column: impl Into<SubscriptionColumnId>) -> EncodedKey {
		Self {
			subscription: subscription.into(),
			column: column.into(),
		}
		.encode()
	}

	/// Returns a range for scanning all columns of a specific subscription
	pub fn subscription_range(subscription: impl Into<SubscriptionId>) -> EncodedKeyRange {
		let subscription = subscription.into();
		EncodedKeyRange::start_end(
			Some(Self::subscription_start(subscription)),
			Some(Self::subscription_end(subscription)),
		)
	}

	fn subscription_start(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 16 (UUID)
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_bytes(subscription.as_bytes());
		serializer.to_encoded_key()
	}

	fn subscription_end(subscription: SubscriptionId) -> EncodedKey {
		// For UUID-based keys, we use the subscription UUID prefix
		// The end bound needs to come after all columns for this subscription
		// Bytes are NOT inverted in extend_bytes, so we INCREMENT to get a larger value
		let mut uuid_bytes = *subscription.as_bytes();
		// Increment the UUID bytes (with wrapping) to get a larger encoded value
		for i in (0..16).rev() {
			if uuid_bytes[i] < 0xFF {
				uuid_bytes[i] = uuid_bytes[i].wrapping_add(1);
				break;
			} else {
				uuid_bytes[i] = 0x00;
			}
		}

		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_bytes(&uuid_bytes);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, SubscriptionColumnKey};
	use crate::interface::catalog::{SubscriptionColumnId, SubscriptionId};

	#[test]
	fn test_encode_decode() {
		let subscription_id = SubscriptionId::new();
		let key = SubscriptionColumnKey {
			subscription: subscription_id,
			column: SubscriptionColumnId(0x1234),
		};
		let encoded = key.encode();

		let decoded = SubscriptionColumnKey::decode(&encoded).unwrap();
		assert_eq!(decoded.subscription, subscription_id);
		assert_eq!(decoded.column, SubscriptionColumnId(0x1234));
	}
}
