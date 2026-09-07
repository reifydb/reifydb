// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, reader::Reader};
use reifydb_core::{
	interface::{
		catalog::metrics::{MetricsId, storage::MultiStorageMetrics},
		store::Tier,
	},
	key::{
		EncodableKey,
		metric::{MetricCdcKey, MetricStorageKey},
	},
};
use reifydb_value::{byte_size::ByteSize, count::Count};

use crate::metrics::storage::cdc::CdcMetrics;

pub fn encode_storage_stats_key(tier: Tier, id: MetricsId) -> EncodedKey {
	MetricStorageKey::encoded(tier, id)
}

pub fn storage_stats_key_prefix() -> EncodedKey {
	MetricStorageKey::prefix()
}

pub fn encode_cdc_stats_key(id: MetricsId) -> EncodedKey {
	MetricCdcKey::encoded(id)
}

pub fn cdc_stats_key_prefix() -> EncodedKey {
	MetricCdcKey::prefix()
}

pub fn decode_storage_stats_key(key: &[u8]) -> Option<(Tier, MetricsId)> {
	let key = MetricStorageKey::decode(&EncodedKey::new(key))?;
	Some((key.tier, key.id))
}

pub fn decode_cdc_stats_key(key: &[u8]) -> Option<MetricsId> {
	MetricCdcKey::decode(&EncodedKey::new(key)).map(|key| key.id)
}

pub const STORAGE_STATS_SIZE: usize = 48;

pub fn encode_storage_stats(stats: &MultiStorageMetrics) -> Vec<u8> {
	let mut buf = Vec::with_capacity(STORAGE_STATS_SIZE);
	buf.extend_from_slice(&stats.estimated_current_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.estimated_current_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.estimated_historical_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.estimated_historical_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.estimated_current_count.to_le_bytes());
	buf.extend_from_slice(&stats.estimated_historical_count.to_le_bytes());
	buf
}

pub fn decode_storage_stats(bytes: &[u8]) -> Option<MultiStorageMetrics> {
	let mut r = Reader::new(bytes);
	Some(MultiStorageMetrics {
		estimated_current_key_bytes: r.u64().ok()?,
		estimated_current_value_bytes: r.u64().ok()?,
		estimated_historical_key_bytes: r.u64().ok()?,
		estimated_historical_value_bytes: r.u64().ok()?,
		estimated_current_count: r.u64().ok()?,
		estimated_historical_count: r.u64().ok()?,
	})
}

pub const CDC_STATS_SIZE: usize = 24;

pub fn encode_cdc_stats(stats: &CdcMetrics) -> Vec<u8> {
	let mut buf = Vec::with_capacity(CDC_STATS_SIZE);
	buf.extend_from_slice(&stats.key_bytes.as_bytes().to_le_bytes());
	buf.extend_from_slice(&stats.value_bytes.as_bytes().to_le_bytes());
	buf.extend_from_slice(&stats.entry_count.as_u64().to_le_bytes());
	buf
}

pub fn decode_cdc_stats(bytes: &[u8]) -> Option<CdcMetrics> {
	let mut r = Reader::new(bytes);
	Some(CdcMetrics {
		key_bytes: ByteSize::from_bytes(r.u64().ok()?),
		value_bytes: ByteSize::from_bytes(r.u64().ok()?),
		entry_count: Count::new(r.u64().ok()?),
	})
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{RingBufferId, SeriesId, TableId},
		object::ObjectId,
	};
	use reifydb_value::value::dictionary::DictionaryId;

	use super::*;

	#[test]
	fn test_storage_stats_key_source_roundtrip() {
		let tier = Tier::Buffer;
		let object_id = ObjectId::Table(TableId(12345));
		let id = MetricsId::Object(object_id);

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_system_roundtrip() {
		let tier = Tier::Persistent;
		let id = MetricsId::System;

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_object_roundtrip_for_every_object_kind() {
		// Table/View/TableVirtual were the only kinds covered before; a discriminant-byte
		// disagreement between encode and decode silently corrupts every other kind's metric id.
		let objects = [
			ObjectId::RingBuffer(RingBufferId(7)),
			ObjectId::Dictionary(DictionaryId(11)),
			ObjectId::Series(SeriesId(13)),
		];

		for object_id in objects {
			let id = MetricsId::Object(object_id);

			let storage_key = encode_storage_stats_key(Tier::Buffer, id);
			let (decoded_tier, decoded_id) = decode_storage_stats_key(&storage_key).unwrap();
			assert_eq!(decoded_tier, Tier::Buffer);
			assert_eq!(decoded_id, id);

			let cdc_key = encode_cdc_stats_key(id);
			let decoded_cdc_id = decode_cdc_stats_key(&cdc_key).unwrap();
			assert_eq!(decoded_cdc_id, id);
		}
	}

	#[test]
	fn test_cdc_stats_key_roundtrip() {
		let object_id = ObjectId::Table(TableId(12345));
		let id = MetricsId::Object(object_id);

		let key = encode_cdc_stats_key(id);
		let decoded = decode_cdc_stats_key(&key).unwrap();

		assert_eq!(decoded, id);
	}

	#[test]
	fn test_storage_stats_roundtrip() {
		let stats = MultiStorageMetrics {
			estimated_current_key_bytes: 100,
			estimated_current_value_bytes: 200,
			estimated_historical_key_bytes: 50,
			estimated_historical_value_bytes: 150,
			estimated_current_count: 10,
			estimated_historical_count: 5,
		};

		let encoded = encode_storage_stats(&stats);
		assert_eq!(encoded.len(), STORAGE_STATS_SIZE);

		let decoded = decode_storage_stats(&encoded).unwrap();
		assert_eq!(decoded, stats);
	}

	#[test]
	fn test_cdc_stats_roundtrip() {
		let stats = CdcMetrics {
			key_bytes: ByteSize::from_bytes(100),
			value_bytes: ByteSize::from_bytes(500),
			entry_count: Count::new(25),
		};

		let encoded = encode_cdc_stats(&stats);
		assert_eq!(encoded.len(), CDC_STATS_SIZE);

		let decoded = decode_cdc_stats(&encoded).unwrap();
		assert_eq!(decoded, stats);
	}

	#[test]
	fn test_key_prefixes() {
		let storage_prefix = storage_stats_key_prefix();
		let cdc_prefix = cdc_stats_key_prefix();

		let storage_key = encode_storage_stats_key(Tier::Buffer, MetricsId::System);
		assert!(storage_key.starts_with(&storage_prefix));

		let cdc_key = encode_cdc_stats_key(MetricsId::System);
		assert!(cdc_key.starts_with(&cdc_prefix));

		assert_ne!(storage_prefix, cdc_prefix);
	}
}
