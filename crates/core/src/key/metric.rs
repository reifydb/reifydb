// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer};

use super::KeyKind;
use crate::{
	interface::{catalog::metrics::MetricsId, store::Tier},
	key::{
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::key::Key,
	},
};

const SUBKEY_STORAGE: u8 = 0x01;
const SUBKEY_CDC: u8 = 0x02;

const ID_OBJECT: u8 = 0x00;
const ID_SYSTEM: u8 = 0x01;

const TIER_BUFFER: u8 = 0x00;
const TIER_PERSISTENT: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricStorageKey {
	pub tier: Tier,
	pub id: MetricsId,
}

impl MetricStorageKey {
	pub fn new(tier: Tier, id: MetricsId) -> Self {
		Self {
			tier,
			id,
		}
	}

	pub fn encoded(tier: Tier, id: MetricsId) -> EncodedKey {
		Self::new(tier, id).encode()
	}

	pub fn prefix() -> EncodedKey {
		subkey_prefix(SUBKEY_STORAGE)
	}
}

impl Key for MetricStorageKey {
	const KIND: KeyKind = KeyKind::Metric;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(13);
		serializer.extend_u8(Self::KIND as u8).extend_u8(SUBKEY_STORAGE).extend_u8(tier_to_byte(self.tier));
		extend_metrics_id(&mut serializer, self.id);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		read_subkey(&mut de, Self::KIND, SUBKEY_STORAGE)?;

		let tier = byte_to_tier(de.read_u8().ok()?)?;
		let id = read_metrics_id(&mut de)?;

		Some(Self {
			tier,
			id,
		})
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricCdcKey {
	pub id: MetricsId,
}

impl MetricCdcKey {
	pub fn new(id: MetricsId) -> Self {
		Self {
			id,
		}
	}

	pub fn encoded(id: MetricsId) -> EncodedKey {
		Self::new(id).encode()
	}

	pub fn prefix() -> EncodedKey {
		subkey_prefix(SUBKEY_CDC)
	}
}

impl Key for MetricCdcKey {
	const KIND: KeyKind = KeyKind::Metric;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(12);
		serializer.extend_u8(Self::KIND as u8).extend_u8(SUBKEY_CDC);
		extend_metrics_id(&mut serializer, self.id);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		read_subkey(&mut de, Self::KIND, SUBKEY_CDC)?;

		let id = read_metrics_id(&mut de)?;

		Some(Self {
			id,
		})
	}
}

fn subkey_prefix(subkey: u8) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(2);
	serializer.extend_u8(KeyKind::Metric as u8).extend_u8(subkey);
	serializer.to_encoded_key()
}

fn read_subkey(de: &mut KeyDeserializer, kind: KeyKind, subkey: u8) -> Option<()> {
	let decoded: KeyKind = de.read_u8().ok()?.try_into().ok()?;
	if decoded != kind {
		return None;
	}
	if de.read_u8().ok()? != subkey {
		return None;
	}
	Some(())
}

fn extend_metrics_id(serializer: &mut KeySerializer, id: MetricsId) {
	match id {
		MetricsId::Object(object) => {
			serializer.extend_u8(ID_OBJECT).extend_object_id(object);
		}
		MetricsId::System => {
			serializer.extend_u8(ID_SYSTEM);
		}
	}
}

fn read_metrics_id(de: &mut KeyDeserializer) -> Option<MetricsId> {
	match de.read_u8().ok()? {
		ID_OBJECT => Some(MetricsId::Object(de.read_object_id().ok()?)),
		ID_SYSTEM => Some(MetricsId::System),
		_ => None,
	}
}

fn tier_to_byte(tier: Tier) -> u8 {
	match tier {
		Tier::Buffer => TIER_BUFFER,
		Tier::Persistent => TIER_PERSISTENT,
	}
}

fn byte_to_tier(byte: u8) -> Option<Tier> {
	match byte {
		TIER_BUFFER => Some(Tier::Buffer),
		TIER_PERSISTENT => Some(Tier::Persistent),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::decode_u8;

	use super::*;
	use crate::interface::catalog::{id::TableId, object::ObjectId};

	#[test]
	fn test_storage_key_is_classified_as_metric() {
		// A leading version byte made KeyKind::of read 0x01 and answer Namespace for every metric key.
		let key = MetricStorageKey::encoded(Tier::Buffer, MetricsId::System);
		assert_eq!(KeyKind::of(&key), Some(KeyKind::Metric));
		assert_eq!(decode_u8(key.as_slice()[0]), KeyKind::Metric as u8);
	}

	#[test]
	fn test_cdc_key_is_classified_as_metric() {
		// Same misclassification: metric CDC keys sorted inside the namespace block.
		let key = MetricCdcKey::encoded(MetricsId::System);
		assert_eq!(KeyKind::of(&key), Some(KeyKind::Metric));
		assert_eq!(decode_u8(key.as_slice()[0]), KeyKind::Metric as u8);
	}

	#[test]
	fn test_storage_key_roundtrip() {
		for tier in [Tier::Buffer, Tier::Persistent] {
			for id in [MetricsId::System, MetricsId::Object(ObjectId::Table(TableId(12345)))] {
				let key = MetricStorageKey::encoded(tier, id);
				assert_eq!(MetricStorageKey::decode(&key), Some(MetricStorageKey::new(tier, id)));
			}
		}
	}

	#[test]
	fn test_cdc_key_roundtrip() {
		for id in [MetricsId::System, MetricsId::Object(ObjectId::Table(TableId(12345)))] {
			let key = MetricCdcKey::encoded(id);
			assert_eq!(MetricCdcKey::decode(&key), Some(MetricCdcKey::new(id)));
		}
	}

	#[test]
	fn test_subkeys_do_not_decode_into_each_other() {
		// Both sub-families share one kind byte; only the sub-key byte keeps them apart.
		let storage = MetricStorageKey::encoded(Tier::Buffer, MetricsId::System);
		let cdc = MetricCdcKey::encoded(MetricsId::System);

		assert!(MetricCdcKey::decode(&storage).is_none());
		assert!(MetricStorageKey::decode(&cdc).is_none());
	}

	#[test]
	fn test_prefixes_match_their_own_family_only() {
		let storage = MetricStorageKey::encoded(Tier::Persistent, MetricsId::System);
		let cdc = MetricCdcKey::encoded(MetricsId::System);

		assert!(storage.as_slice().starts_with(MetricStorageKey::prefix().as_slice()));
		assert!(cdc.as_slice().starts_with(MetricCdcKey::prefix().as_slice()));
		assert!(!storage.as_slice().starts_with(MetricCdcKey::prefix().as_slice()));
		assert!(!cdc.as_slice().starts_with(MetricStorageKey::prefix().as_slice()));
	}

	#[test]
	fn test_byte_order_matches_descending_object_order() {
		// Keys are read back by range scan, so byte order has to follow the house descending order.
		let low = MetricStorageKey::encoded(Tier::Buffer, MetricsId::Object(ObjectId::Table(TableId(1))));
		let high = MetricStorageKey::encoded(Tier::Buffer, MetricsId::Object(ObjectId::Table(TableId(2))));
		assert!(high.as_slice() < low.as_slice());

		let buffer = MetricStorageKey::encoded(Tier::Buffer, MetricsId::System);
		let persistent = MetricStorageKey::encoded(Tier::Persistent, MetricsId::System);
		assert!(persistent.as_slice() < buffer.as_slice());
	}
}
