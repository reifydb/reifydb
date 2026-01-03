// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::catalog::SubscriptionId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

/// Key for subscription delta entries.
/// Ordered by (subscription_id, version, sequence) for efficient range queries.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionDeltaKey {
	pub subscription: SubscriptionId,
	pub version: CommitVersion,
	pub sequence: u16,
}

const VERSION: u8 = 1;

impl EncodableKey for SubscriptionDeltaKey {
	const KIND: KeyKind = KeyKind::SubscriptionDelta;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(28); // 1 + 1 + 16 (UUID) + 8 + 2
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(self.subscription.as_bytes())
			.extend_u64(self.version)
			.extend_u16(self.sequence);
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
		let commit_version = de.read_u64().ok()?;
		let sequence = de.read_u16().ok()?;

		Some(Self {
			subscription: SubscriptionId::from_bytes(uuid_bytes),
			version: CommitVersion(commit_version),
			sequence,
		})
	}
}

impl SubscriptionDeltaKey {
	pub fn encoded(subscription: impl Into<SubscriptionId>, version: CommitVersion, sequence: u16) -> EncodedKey {
		Self {
			subscription: subscription.into(),
			version,
			sequence,
		}
		.encode()
	}

	/// Range for scanning all deltas of a subscription
	pub fn subscription_scan(subscription: SubscriptionId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::subscription_start(subscription)),
			Some(Self::subscription_end(subscription)),
		)
	}

	/// Range for scanning deltas of a subscription after a specific version
	pub fn subscription_after_version(
		subscription: SubscriptionId,
		after_version: CommitVersion,
	) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::version_start(subscription, CommitVersion(after_version.0 + 1))),
			Some(Self::subscription_end(subscription)),
		)
	}

	/// Range for scanning deltas of a subscription up to and including a specific version
	pub fn subscription_up_to_version(
		subscription: SubscriptionId,
		up_to_version: CommitVersion,
	) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::subscription_start(subscription)),
			Some(Self::version_end(subscription, up_to_version)),
		)
	}

	fn subscription_start(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 16 (UUID)
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_bytes(subscription.as_bytes());
		serializer.to_encoded_key()
	}

	fn subscription_end(subscription: SubscriptionId) -> EncodedKey {
		// Bytes are NOT inverted in extend_bytes, so we INCREMENT to get a larger value
		let mut uuid_bytes = *subscription.as_bytes();
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

	fn version_start(subscription: SubscriptionId, version: CommitVersion) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26); // 1 + 1 + 16 (UUID) + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(subscription.as_bytes())
			.extend_u64(version);
		serializer.to_encoded_key()
	}

	fn version_end(subscription: SubscriptionId, version: CommitVersion) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(subscription.as_bytes())
			.extend_u64(version.0.wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, SubscriptionDeltaKey};
	use crate::{CommitVersion, interface::catalog::SubscriptionId};

	#[test]
	fn test_encode_decode() {
		let subscription_id = SubscriptionId::new();
		let key = SubscriptionDeltaKey {
			subscription: subscription_id,
			version: CommitVersion(100),
			sequence: 5,
		};
		let encoded = key.encode();

		let decoded = SubscriptionDeltaKey::decode(&encoded).unwrap();
		assert_eq!(decoded.subscription, subscription_id);
		assert_eq!(decoded.version, CommitVersion(100));
		assert_eq!(decoded.sequence, 5);
	}

	#[test]
	fn test_ordering() {
		// Earlier version should sort before later version (descending order in keycode)
		let subscription_id = SubscriptionId::new();
		let key1 = SubscriptionDeltaKey {
			subscription: subscription_id,
			version: CommitVersion(100),
			sequence: 0,
		};
		let key2 = SubscriptionDeltaKey {
			subscription: subscription_id,
			version: CommitVersion(200),
			sequence: 0,
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		// In descending order, larger values have smaller byte representations
		assert!(encoded2 < encoded1);
	}
}
