// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{RingBufferId, SeriesId, TableId, ViewId},
			shape::ShapeId,
			vtable::VTableId,
		},
		store::Tier,
	},
	key::kind::KeyKind,
};
use reifydb_type::value::dictionary::DictionaryId;

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

pub fn encode_type_stats_key(tier: Tier, kind: KeyKind) -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_TYPE, tier_to_byte(tier), kind as u8]
}

pub fn encode_storage_stats_key(tier: Tier, id: MetricId) -> Vec<u8> {
	let mut key = vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_OBJECT, tier_to_byte(tier)];
	encode_object_id(&mut key, id);
	key
}

pub fn type_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_TYPE]
}

pub fn storage_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_OBJECT]
}

pub fn encode_cdc_stats_key(id: MetricId) -> Vec<u8> {
	let mut key = vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_CDC];
	encode_object_id(&mut key, id);
	key
}

pub fn cdc_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_CDC]
}

pub fn decode_type_stats_key(key: &[u8]) -> Option<(Tier, KeyKind)> {
	if key.len() < 5 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::Metric as u8 || key[2] != SUBKEY_BY_TYPE {
		return None;
	}
	let tier = byte_to_tier(key[3])?;
	let kind = KeyKind::try_from(key[4]).ok()?;
	Some((tier, kind))
}

pub fn decode_storage_stats_key(key: &[u8]) -> Option<(Tier, MetricId)> {
	if key.len() < 5 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::Metric as u8 || key[2] != SUBKEY_BY_OBJECT {
		return None;
	}
	let tier = byte_to_tier(key[3])?;
	let id = decode_object_id(&key[4..])?;
	Some((tier, id))
}

pub fn decode_cdc_stats_key(key: &[u8]) -> Option<MetricId> {
	if key.len() < 4 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::Metric as u8 || key[2] != SUBKEY_CDC {
		return None;
	}
	decode_object_id(&key[3..])
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
	if bytes.len() < STORAGE_STATS_SIZE {
		return None;
	}
	Some(MultiStorageStats {
		current_key_bytes: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
		current_value_bytes: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
		historical_key_bytes: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
		historical_value_bytes: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
		current_count: u64::from_le_bytes(bytes[32..40].try_into().ok()?),
		historical_count: u64::from_le_bytes(bytes[40..48].try_into().ok()?),
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
	if bytes.len() < CDC_STATS_SIZE {
		return None;
	}
	Some(CdcStats {
		key_bytes: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
		value_bytes: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
		entry_count: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
	})
}

fn tier_to_byte(tier: Tier) -> u8 {
	match tier {
		Tier::Hot => 0x00,
		Tier::Warm => 0x01,
		Tier::Cold => 0x02,
	}
}

fn byte_to_tier(b: u8) -> Option<Tier> {
	match b {
		0x00 => Some(Tier::Hot),
		0x01 => Some(Tier::Warm),
		0x02 => Some(Tier::Cold),
		_ => None,
	}
}

fn encode_object_id(buf: &mut Vec<u8>, id: MetricId) {
	match id {
		MetricId::Shape(shape_id) => {
			buf.push(ID_SHAPE);
			buf.extend_from_slice(&encode_shape_id(shape_id));
		}
		MetricId::FlowNode(flow_node_id) => {
			buf.push(ID_FLOW_NODE);
			buf.extend_from_slice(&flow_node_id.0.to_le_bytes());
		}
		MetricId::System => {
			buf.push(ID_SYSTEM);
		}
	}
}

fn decode_object_id(bytes: &[u8]) -> Option<MetricId> {
	if bytes.is_empty() {
		return None;
	}
	match bytes[0] {
		ID_SHAPE => {
			if bytes.len() < 10 {
				return None;
			}
			let shape_id = decode_shape_id(&bytes[1..10])?;
			Some(MetricId::Shape(shape_id))
		}
		ID_FLOW_NODE => {
			if bytes.len() < 9 {
				return None;
			}
			let id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
			Some(MetricId::FlowNode(FlowNodeId(id)))
		}
		ID_SYSTEM => Some(MetricId::System),
		_ => None,
	}
}

fn encode_shape_id(shape_id: ShapeId) -> [u8; 9] {
	let mut buf = [0u8; 9];
	buf[0] = shape_id.to_type_u8();
	buf[1..9].copy_from_slice(&shape_id.as_u64().to_be_bytes());
	buf
}

fn decode_shape_id(bytes: &[u8]) -> Option<ShapeId> {
	if bytes.len() < 9 {
		return None;
	}
	let discriminant = bytes[0];
	let id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);

	match discriminant {
		1 => Some(ShapeId::Table(TableId(id))),
		2 => Some(ShapeId::View(ViewId(id))),
		3 => Some(ShapeId::TableVirtual(VTableId(id))),
		5 => Some(ShapeId::RingBuffer(RingBufferId(id))),
		6 => Some(ShapeId::Dictionary(DictionaryId(id))),
		7 => Some(ShapeId::Series(SeriesId(id))),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{flow::FlowNodeId, id::TableId, shape::ShapeId};

	use super::*;

	#[test]
	fn test_type_stats_key_roundtrip() {
		let tier = Tier::Warm;
		let kind = KeyKind::Row;

		let key = encode_type_stats_key(tier, kind);
		let decoded = decode_type_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, kind));
	}

	#[test]
	fn test_storage_stats_key_source_roundtrip() {
		let tier = Tier::Hot;
		let shape_id = ShapeId::Table(TableId(12345));
		let id = MetricId::Shape(shape_id);

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_flow_node_roundtrip() {
		let tier = Tier::Cold;
		let id = MetricId::FlowNode(FlowNodeId(999));

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
	}

	#[test]
	fn test_storage_stats_key_system_roundtrip() {
		let tier = Tier::Warm;
		let id = MetricId::System;

		let key = encode_storage_stats_key(tier, id);
		let decoded = decode_storage_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, id));
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

		// Type stats key should start with type prefix
		let type_key = encode_type_stats_key(Tier::Hot, KeyKind::Row);
		assert!(type_key.starts_with(&type_prefix));

		// Storage stats key should start with storage prefix
		let storage_key = encode_storage_stats_key(Tier::Hot, MetricId::System);
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
