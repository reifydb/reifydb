// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{EncodableKey, KeyKind};
use crate::{EncodedKey, interface::cdc::ConsumerId, util::encoding::keycode};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CdcConsumerKey {
	pub consumer: ConsumerId,
}

const VERSION_BYTE: u8 = 1;

impl EncodableKey for CdcConsumerKey {
	const KIND: KeyKind = KeyKind::CdcConsumer;

	fn encode(&self) -> EncodedKey {
		let mut out = Vec::with_capacity(10);
		out.extend(&keycode::serialize(&VERSION_BYTE));
		out.extend(&keycode::serialize(&Self::KIND));
		out.extend(&keycode::serialize(&self.consumer.0));
		EncodedKey::new(out)
	}

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		if key.len() < 10 {
			return None;
		}

		let version: u8 = keycode::deserialize(&key[0..1]).ok()?;
		if version != VERSION_BYTE {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		if kind != Self::KIND {
			return None;
		}

		let consumer_id: u64 =
			keycode::deserialize(&key[2..10]).ok()?;

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
			consumer: ConsumerId(42),
		};

		let encoded = key.encode();
		let decoded = CdcConsumerKey::decode(&encoded)
			.expect("Failed to decode key");

		assert_eq!(decoded.consumer, ConsumerId(42));
	}
}
