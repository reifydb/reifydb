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
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 8 (subscription u64) + 8 (row)
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.subscription.0)
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

		let subscription_id = de.read_u64().ok()?;
		let subscription = SubscriptionId(subscription_id);
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

		let subscription_id = de.read_u64().ok()?;
		let subscription = SubscriptionId(subscription_id);

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
		let mut serializer = KeySerializer::with_capacity(10); // 1 + 1 + 8 (subscription u64)
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.subscription.0);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 8 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.subscription.0)
			.extend_u64(0u64);
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
		EncodedKeyRange::start_end(Some(Self::start(subscription)), Some(Self::end(subscription)))
	}

	fn start(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 8 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(subscription.0)
			.extend_u64(u64::MAX);
		serializer.to_encoded_key()
	}

	fn end(subscription: SubscriptionId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18); // 1 + 1 + 8 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(subscription.0)
			.extend_u64(0u64);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::row_number::RowNumber;

	use super::{EncodableKey, SubscriptionRowKey};
	use crate::interface::catalog::id::SubscriptionId;

	#[test]
	fn test_encode_decode() {
		let subscription = SubscriptionId(12345);
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
		let sub1 = SubscriptionId(100);
		let sub2 = SubscriptionId(101);

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

		assert!(encoded1 > encoded2, "row ordering not preserved");
		assert!(encoded1 > encoded3, "subscription ordering not preserved");
	}
}
