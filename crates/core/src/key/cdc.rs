// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;

use super::KeyTag;
use crate::{
	interface::cdc::CdcConsumerId,
	key::{
		any::{ByteEncoding, Field, KeyFields},
		bound::TaggedKeyBoundRange,
	},
};

pub trait ToConsumerKey {
	fn to_consumer_key(&self) -> CdcConsumerKey;
}

impl ToConsumerKey for CdcConsumerKey {
	fn to_consumer_key(&self) -> CdcConsumerKey {
		self.clone()
	}
}

impl ToConsumerKey for CdcConsumerId {
	fn to_consumer_key(&self) -> CdcConsumerKey {
		CdcConsumerKey {
			consumer: self.clone(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Eq, PartialOrd, Ord, Hash)]
#[key(tag = CdcConsumer)]
pub struct CdcConsumerKey {
	pub consumer: CdcConsumerId,
}

impl CdcConsumerKey {
	pub fn new(consumer: impl Into<CdcConsumerId>) -> Self {
		Self {
			consumer: consumer.into(),
		}
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod cdc_consumer_key_tests {
	use std::ops::RangeBounds;

	use super::CdcConsumerKey;
	use crate::interface::cdc::CdcConsumerId;

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

		let range = CdcConsumerKey::full_scan().encode();

		assert!(range.contains(&key1), "consumer-a key should be in range");
		assert!(range.contains(&key2), "consumer-b key should be in range");
		assert!(range.contains(&key3), "consumer-z key should be in range");
	}
}
