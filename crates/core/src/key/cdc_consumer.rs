// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey, EncodedKeyRange,
	interface::CdcConsumerId,
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

/// Trait for types that can be converted to a consumer key
pub trait ToConsumerKey {
	fn to_consumer_key(&self) -> EncodedKey;
}

impl ToConsumerKey for EncodedKey {
	fn to_consumer_key(&self) -> EncodedKey {
		self.clone()
	}
}

impl ToConsumerKey for CdcConsumerId {
	fn to_consumer_key(&self) -> EncodedKey {
		CdcConsumerKey {
			consumer: self.clone(),
		}
		.encode()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CdcConsumerKey {
	pub consumer: CdcConsumerId,
}

pub const VERSION_BYTE: u8 = 1;

impl EncodableKey for CdcConsumerKey {
	const KIND: KeyKind = KeyKind::CdcConsumer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(VERSION_BYTE).extend_u8(Self::KIND as u8).extend_str(&self.consumer);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION_BYTE {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let consumer_id = de.read_str().ok()?;

		Some(Self {
			consumer: CdcConsumerId(consumer_id),
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcConsumerKeyRange;

impl CdcConsumerKeyRange {
	/// Creates a key range that spans all CDC consumer checkpoint keys
	///
	/// Returns an `EncodedKeyRange` that can be used with transaction
	/// range scan operations to iterate over all registered CDC consumers.
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION_BYTE).extend_u8(CdcConsumerKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION_BYTE).extend_u8((CdcConsumerKey::KIND as u8).wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::{CdcConsumerKey, CdcConsumerKeyRange, EncodableKey};
	use crate::interface::CdcConsumerId;

	#[test]
	fn test_encode_decode_cdc_consumer() {
		let key = CdcConsumerKey {
			consumer: CdcConsumerId::new("test-consumer"),
		};

		let encoded = key.encode();
		let decoded = CdcConsumerKey::decode(&encoded).expect("Failed to decode key");

		assert_eq!(decoded.consumer, CdcConsumerId::new("test-consumer"));
	}

	#[test]
	fn test_cdc_consumer_keys_within_range() {
		// Create several CDC consumer keys
		let key1 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-a"),
		}
		.encode();

		let key2 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-b"),
		}
		.encode();

		let key3 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-z"),
		}
		.encode();

		// Get the range
		let range = CdcConsumerKeyRange::full_scan();

		// All CDC consumer keys should fall within the range
		assert!(range.contains(&key1), "consumer-a key should be in range");
		assert!(range.contains(&key2), "consumer-b key should be in range");
		assert!(range.contains(&key3), "consumer-z key should be in range");
	}
}
