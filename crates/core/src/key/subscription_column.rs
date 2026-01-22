// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{SubscriptionColumnId, SubscriptionId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
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
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 8 (subscription u64) + 8 (column)
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.subscription.0)
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

		let subscription_id = de.read_u64().ok()?;
		let column = de.read_u64().ok()?;

		Some(Self {
			subscription: SubscriptionId(subscription_id),
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
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(subscription.0);
		serializer.to_encoded_key()
	}

	fn subscription_end(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(subscription.0.wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, SubscriptionColumnKey};
	use crate::interface::catalog::id::{SubscriptionColumnId, SubscriptionId};

	#[test]
	fn test_encode_decode() {
		let subscription_id = SubscriptionId(12345);
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
