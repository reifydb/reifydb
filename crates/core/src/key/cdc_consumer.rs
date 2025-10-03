// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{
	EncodedKey,
	interface::CdcConsumerId,
	util::encoding::keycode::{KeySerializer, deserialize},
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

const VERSION_BYTE: u8 = 1;

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
		if key.len() < 2 {
			return None;
		}

		let version: u8 = deserialize(&key[0..1]).ok()?;
		if version != VERSION_BYTE {
			return None;
		}

		let kind: KeyKind = deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let consumer_id: String = deserialize(&key[2..]).ok()?;

		Some(Self {
			consumer: CdcConsumerId(consumer_id),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{CdcConsumerKey, EncodableKey};
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
}
