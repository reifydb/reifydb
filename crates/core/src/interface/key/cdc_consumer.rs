// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use keycode::{deserialize, serialize};

use super::{EncodableKey, KeyKind};
use crate::{EncodedKey, interface::cdc::ConsumerId, util::encoding::keycode};

/// Trait for types that can be converted to a consumer key
pub trait ToConsumerKey {
	fn to_consumer_key(&self) -> EncodedKey;
}

impl ToConsumerKey for EncodedKey {
	fn to_consumer_key(&self) -> EncodedKey {
		self.clone()
	}
}

impl ToConsumerKey for ConsumerId {
	fn to_consumer_key(&self) -> EncodedKey {
		CdcConsumerKey {
			consumer: self.clone(),
		}
		.encode()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CdcConsumerKey {
	pub consumer: ConsumerId,
}

const VERSION_BYTE: u8 = 1;

impl EncodableKey for CdcConsumerKey {
	const KIND: KeyKind = KeyKind::CdcConsumer;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::new();
		out.extend(&serialize(&VERSION_BYTE));
		out.extend(&serialize(&Self::KIND));
		out.extend(&serialize(&self.consumer.0));
		EncodedKey::new(out)
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
			consumer: ConsumerId(consumer_id),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{CdcConsumerKey, EncodableKey};
	use crate::interface::cdc::ConsumerId;

	#[test]
	fn test_encode_decode_cdc_consumer() {
		let key = CdcConsumerKey {
			consumer: ConsumerId::new("test-consumer"),
		};

		let encoded = key.encode();
		let decoded = CdcConsumerKey::decode(&encoded)
			.expect("Failed to decode key");

		assert_eq!(decoded.consumer, ConsumerId::new("test-consumer"));
	}
}
