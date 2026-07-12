// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{
		deserializer::KeyDeserializer,
		encoded::{EncodedKey, EncodedKeyBuilder},
	},
	reader::Reader,
};
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, store::Tier},
	key::{
		catalog::{EncodedKeyBuilderCatalogExt, KeyDeserializerCatalogExt},
		kind::KeyKind,
	},
	profiler::ProfilerCategoryId,
};

use crate::{
	MetricId,
	storage::{cdc::CdcStats, multi::MultiStorageStats},
};

const KEY_VERSION: u8 = 0x01;

const SUBKEY_BY_TYPE: u8 = 0x01;
const SUBKEY_BY_OBJECT: u8 = 0x02;
const SUBKEY_CDC: u8 = 0x03;

const ID_SHAPE: u8 = 0x00;
const ID_FLOW_NODE: u8 = 0x01;
const ID_SYSTEM: u8 = 0x02;
const ID_PROFILE: u8 = 0x03;

pub fn encode_type_stats_key(tier: Tier, kind: KeyKind) -> EncodedKey {
	EncodedKeyBuilder::new()
		.u8(KEY_VERSION)
		.u8(KeyKind::Metric as u8)
		.u8(SUBKEY_BY_TYPE)
		.u8(tier_to_byte(tier))
		.u8(kind as u8)
		.build()
}

pub fn encode_storage_stats_key(tier: Tier, id: MetricId) -> EncodedKey {
	let builder = EncodedKeyBuilder::new()
		.u8(KEY_VERSION)
		.u8(KeyKind::Metric as u8)
		.u8(SUBKEY_BY_OBJECT)
		.u8(tier_to_byte(tier));
	extend_object_id(builder, id).build()
}

pub fn type_stats_key_prefix() -> EncodedKey {
	EncodedKeyBuilder::new().u8(KEY_VERSION).u8(KeyKind::Metric as u8).u8(SUBKEY_BY_TYPE).build()
}

pub fn storage_stats_key_prefix() -> EncodedKey {
	EncodedKeyBuilder::new().u8(KEY_VERSION).u8(KeyKind::Metric as u8).u8(SUBKEY_BY_OBJECT).build()
}

pub fn encode_cdc_stats_key(id: MetricId) -> EncodedKey {
	let builder = EncodedKeyBuilder::new().u8(KEY_VERSION).u8(KeyKind::Metric as u8).u8(SUBKEY_CDC);
	extend_object_id(builder, id).build()
}

pub fn cdc_stats_key_prefix() -> EncodedKey {
	EncodedKeyBuilder::new().u8(KEY_VERSION).u8(KeyKind::Metric as u8).u8(SUBKEY_CDC).build()
}

pub fn decode_type_stats_key(key: &[u8]) -> Option<(Tier, KeyKind)> {
	let mut de = KeyDeserializer::from_bytes(key);
	if de.read_u8().ok()? != KEY_VERSION {
		return None;
	}
	if de.read_u8().ok()? != KeyKind::Metric as u8 {
		return None;
	}
	if de.read_u8().ok()? != SUBKEY_BY_TYPE {
		return None;
	}
	let tier = byte_to_tier(de.read_u8().ok()?)?;
	let kind = KeyKind::try_from(de.read_u8().ok()?).ok()?;
	Some((tier, kind))
}

pub fn decode_storage_stats_key(key: &[u8]) -> Option<(Tier, MetricId)> {
	let mut de = KeyDeserializer::from_bytes(key);
	if de.read_u8().ok()? != KEY_VERSION {
		return None;
	}
	if de.read_u8().ok()? != KeyKind::Metric as u8 {
		return None;
	}
	if de.read_u8().ok()? != SUBKEY_BY_OBJECT {
		return None;
	}
	let tier = byte_to_tier(de.read_u8().ok()?)?;
	let id = decode_object_id(&mut de)?;
	Some((tier, id))
}

pub fn decode_cdc_stats_key(key: &[u8]) -> Option<MetricId> {
	let mut de = KeyDeserializer::from_bytes(key);
	if de.read_u8().ok()? != KEY_VERSION {
		return None;
	}
	if de.read_u8().ok()? != KeyKind::Metric as u8 {
		return None;
	}
	if de.read_u8().ok()? != SUBKEY_CDC {
		return None;
	}
	decode_object_id(&mut de)
}

pub const STORAGE_STATS_SIZE: usize = 48;

pub fn encode_storage_stats(stats: &MultiStorageStats) -> Vec<u8> {
	let mut buf = Vec::with_capacity(STORAGE_STATS_SIZE);
	buf.extend_from_slice(&stats.current_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.current_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.historical_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.historical_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.current_count.to_le_bytes());
	buf.extend_from_slice(&stats.historical_count.to_le_bytes());
	buf
}

pub fn decode_storage_stats(bytes: &[u8]) -> Option<MultiStorageStats> {
	let mut r = Reader::new(bytes);
	Some(MultiStorageStats {
		current_key_bytes: r.u64().ok()?,
		current_value_bytes: r.u64().ok()?,
		historical_key_bytes: r.u64().ok()?,
		historical_value_bytes: r.u64().ok()?,
		current_count: r.u64().ok()?,
		historical_count: r.u64().ok()?,
	})
}

pub const CDC_STATS_SIZE: usize = 24;

pub fn encode_cdc_stats(stats: &CdcStats) -> Vec<u8> {
	let mut buf = Vec::with_capacity(CDC_STATS_SIZE);
	buf.extend_from_slice(&stats.key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.entry_count.to_le_bytes());
	buf
}

pub fn decode_cdc_stats(bytes: &[u8]) -> Option<CdcStats> {
	let mut r = Reader::new(bytes);
	Some(CdcStats {
		key_bytes: r.u64().ok()?,
		value_bytes: r.u64().ok()?,
		entry_count: r.u64().ok()?,
	})
}

fn tier_to_byte(tier: Tier) -> u8 {
	match tier {
		Tier::Buffer => 0x00,
		Tier::Persistent => 0x01,
	}
}

fn byte_to_tier(b: u8) -> Option<Tier> {
	match b {
		0x00 => Some(Tier::Buffer),
		0x01 => Some(Tier::Persistent),
		_ => None,
	}
}

fn extend_object_id(builder: EncodedKeyBuilder, id: MetricId) -> EncodedKeyBuilder {
	match id {
		MetricId::Shape(shape_id) => builder.u8(ID_SHAPE).shape_id(shape_id),
		MetricId::FlowNode(flow_node_id) => builder.u8(ID_FLOW_NODE).u64(flow_node_id.0),
		MetricId::System => builder.u8(ID_SYSTEM),
		MetricId::Profiler(cat_id) => builder.u8(ID_PROFILE).u8(cat_id.0),
	}
}

fn decode_object_id(de: &mut KeyDeserializer) -> Option<MetricId> {
	match de.read_u8().ok()? {
		ID_SHAPE => Some(MetricId::Shape(de.read_shape_id().ok()?)),
		ID_FLOW_NODE => Some(MetricId::FlowNode(FlowNodeId(de.read_u64().ok()?))),
		ID_SYSTEM => Some(MetricId::System),
		ID_PROFILE => Some(MetricId::Profiler(ProfilerCategoryId(de.read_u8().ok()?))),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		flow::FlowNodeId,
		id::{RingBufferId, SeriesId, TableId},
		shape::ShapeId,
	};
	use reifydb_value::value::dictionary::DictionaryId;

	use super::*;

	#[test]
	fn test_type_stats_key_roundtrip() {
		let tier = Tier::Persistent;
		let kind = KeyKind::Row;

		let key = encode_type_stats_key(tier, kind);
		let decoded = decode_type_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, kind));
	}

	#[test]
	fn test_storage_stats_key_source_roundtrip() {
		let tier = Tier::Buffer;
		let shape_id = ShapeId::Table(TableId(12345));
		let id = MetricId::Shape(shape_id);

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_flow_node_roundtrip() {
		let tier = Tier::Persistent;
		let id = MetricId::FlowNode(FlowNodeId(999));

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_system_roundtrip() {
		let tier = Tier::Persistent;
		let id = MetricId::System;

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_shape_roundtrip_for_every_shape_kind() {
		// Regression test: encode_shape_id/decode_shape_id used to disagree on the discriminant
		// byte for every shape kind but Table/View/TableVirtual, silently corrupting RingBuffer,
		// Dictionary and Series metric ids. Now backed by the shared, tested ShapeId codec.
		let shapes = [
			ShapeId::RingBuffer(RingBufferId(7)),
			ShapeId::Dictionary(DictionaryId(11)),
			ShapeId::Series(SeriesId(13)),
		];

		for shape_id in shapes {
			let id = MetricId::Shape(shape_id);

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
	fn test_storage_stats_key_profiler_roundtrip() {
		let tier = Tier::Persistent;
		let id = MetricId::Profiler(ProfilerCategoryId(42));

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();
		assert_eq!(decoded, (tier, id));

		let cdc_key = encode_cdc_stats_key(id);
		let decoded_cdc_id = decode_cdc_stats_key(&cdc_key).unwrap();
		assert_eq!(decoded_cdc_id, id);
	}

	#[test]
	fn test_cdc_stats_key_roundtrip() {
		let shape_id = ShapeId::Table(TableId(12345));
		let id = MetricId::Shape(shape_id);

		let key = encode_cdc_stats_key(id);
		let decoded = decode_cdc_stats_key(&key).unwrap();

		assert_eq!(decoded, id);
	}

	#[test]
	fn test_storage_stats_roundtrip() {
		let stats = MultiStorageStats {
			current_key_bytes: 100,
			current_value_bytes: 200,
			historical_key_bytes: 50,
			historical_value_bytes: 150,
			current_count: 10,
			historical_count: 5,
		};

		let encoded = encode_storage_stats(&stats);
		assert_eq!(encoded.len(), STORAGE_STATS_SIZE);

		let decoded = decode_storage_stats(&encoded).unwrap();
		assert_eq!(decoded, stats);
	}

	#[test]
	fn test_cdc_stats_roundtrip() {
		let stats = CdcStats {
			key_bytes: 100,
			value_bytes: 500,
			entry_count: 25,
		};

		let encoded = encode_cdc_stats(&stats);
		assert_eq!(encoded.len(), CDC_STATS_SIZE);

		let decoded = decode_cdc_stats(&encoded).unwrap();
		assert_eq!(decoded, stats);
	}

	#[test]
	fn test_key_prefixes() {
		let type_prefix = type_stats_key_prefix();
		let storage_prefix = storage_stats_key_prefix();
		let cdc_prefix = cdc_stats_key_prefix();

		// ValueType stats key should start with type prefix
		let type_key = encode_type_stats_key(Tier::Buffer, KeyKind::Row);
		assert!(type_key.starts_with(&type_prefix));

		// Storage stats key should start with storage prefix
		let storage_key = encode_storage_stats_key(Tier::Buffer, MetricId::System);
		assert!(storage_key.starts_with(&storage_prefix));

		// CDC stats key should start with cdc prefix
		let cdc_key = encode_cdc_stats_key(MetricId::System);
		assert!(cdc_key.starts_with(&cdc_prefix));

		// All prefixes should be different
		assert_ne!(type_prefix, storage_prefix);
		assert_ne!(type_prefix, cdc_prefix);
		assert_ne!(storage_prefix, cdc_prefix);
	}
}
