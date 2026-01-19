// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use reifydb_type::value::row_number::RowNumber;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::SubscriptionId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionRowKey {
	pub subscription: SubscriptionId,
	pub row: RowNumber,
}

impl EncodableKey for SubscriptionRowKey {
	const KIND: KeyKind = KeyKind::SubscriptionRow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26); // 1 + 1 + 16 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(self.subscription.as_bytes())
			.extend_u64(self.row.0);
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

		let bytes = de.read_bytes().ok()?;
		let uuid_bytes: [u8; 16] = bytes.try_into().ok()?;
		let subscription = SubscriptionId::from_bytes(uuid_bytes);
		let row = de.read_row_number().ok()?;

		Some(Self {
			subscription,
			row,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionRowKeyRange {
	pub subscription: SubscriptionId,
}

impl SubscriptionRowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let bytes = de.read_bytes().ok()?;
		let uuid_bytes: [u8; 16] = bytes.try_into().ok()?;
		let subscription = SubscriptionId::from_bytes(uuid_bytes);

		Some(SubscriptionRowKeyRange {
			subscription,
		})
	}

	/// Create a range for scanning rows from a subscription
	///
	/// If `last_key` is provided, creates a range that continues from after that key.
	/// Otherwise, creates a range that includes all rows for the subscription.
	///
	/// The caller is responsible for limiting the number of results returned.
	pub fn scan_range(subscription: SubscriptionId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = SubscriptionRowKeyRange {
			subscription,
		};

		if let Some(last_key) = last_key {
			EncodedKeyRange::new(Bound::Excluded(last_key.clone()), Bound::Included(range.end().unwrap()))
		} else {
			EncodedKeyRange::new(
				Bound::Included(range.start().unwrap()),
				Bound::Included(range.end().unwrap()),
			)
		}
	}
}

impl EncodableKeyRange for SubscriptionRowKeyRange {
	const KIND: KeyKind = KeyKind::SubscriptionRow;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 16
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_bytes(self.subscription.as_bytes());
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(self.subscription.as_bytes())
			.extend_u64(0u64); // End at minimum row number (largest bytes in descending encoding)
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl SubscriptionRowKey {
	pub fn encoded(subscription: SubscriptionId, row: impl Into<RowNumber>) -> EncodedKey {
		Self {
			subscription,
			row: row.into(),
		}
		.encode()
	}

	pub fn full_scan(subscription: SubscriptionId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::start(subscription)),
			Some(Self::end(subscription)),
		)
	}

	fn start(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(subscription.as_bytes())
			.extend_u64(u64::MAX);
		serializer.to_encoded_key()
	}

	fn end(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_bytes(subscription.as_bytes())
			.extend_u64(0u64);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::row_number::RowNumber;
	use uuid::Uuid;

	use super::{EncodableKey, SubscriptionRowKey};
	use crate::interface::catalog::id::SubscriptionId;

	#[test]
	fn test_encode_decode() {
		let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
		let subscription = SubscriptionId::from(uuid);
		let key = SubscriptionRowKey {
			subscription,
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let decoded = SubscriptionRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.subscription, subscription);
		assert_eq!(decoded.row, RowNumber(0x123456789ABCDEF0));
	}

	#[test]
	fn test_order_preserving() {
		let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
		let uuid2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

		let sub1 = SubscriptionId::from(uuid1);
		let sub2 = SubscriptionId::from(uuid2);

		let key1 = SubscriptionRowKey {
			subscription: sub1,
			row: RowNumber(100),
		};
		let key2 = SubscriptionRowKey {
			subscription: sub1,
			row: RowNumber(200),
		};
		let key3 = SubscriptionRowKey {
			subscription: sub2,
			row: RowNumber(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		// Same subscription, different rows - with descending u64 encoding, higher row numbers encode to
		// smaller bytes
		assert!(encoded1 > encoded2, "row ordering not preserved");

		// Different subscriptions - ordering depends on UUID
		assert!(encoded1 < encoded3, "subscription ordering not preserved");
	}
}
