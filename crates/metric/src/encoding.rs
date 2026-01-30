// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Persistence key/value encoding for metrics.
//!
//! Key format: All keys start with `[VERSION:1][KeyKind::Metric = 0x25]`
//! followed by a sub-type discriminator:
//!
//! - `[VERSION][0x25][0x01][tier:1][key_kind:1]` -> StorageStats for (tier, KeyKind)
//! - `[VERSION][0x25][0x02][tier:1][id:variable]` -> StorageStats for (tier, Id)
//! - `[VERSION][0x25][0x03][id:variable]` -> CdcStats for Id (no tier)

use reifydb_core::{
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{RingBufferId, TableId, ViewId},
		primitive::PrimitiveId,
		vtable::VTableId,
	},
	key::kind::KeyKind,
};
use reifydb_type::value::dictionary::DictionaryId;

use crate::{
	MetricId,
	cdc::CdcStats,
	multi::{MultiStorageStats, Tier},
};

/// Current key encoding version
const KEY_VERSION: u8 = 0x01;

/// Sub-key discriminators
const SUBKEY_BY_TYPE: u8 = 0x01;
const SUBKEY_BY_OBJECT: u8 = 0x02;
const SUBKEY_CDC: u8 = 0x03;

/// id discriminators for encoding
const ID_SOURCE: u8 = 0x00;
const ID_FLOW_NODE: u8 = 0x01;
const ID_SYSTEM: u8 = 0x02;

// ============================================================================
// Multi Storage Stats Key Encoding
// ============================================================================

/// Encode a per-type stats key.
/// Format: `[VERSION][0x25][0x01][tier:1][key_kind:1]`
pub fn encode_type_stats_key(tier: Tier, kind: KeyKind) -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_TYPE, tier_to_byte(tier), kind as u8]
}

/// Encode a per-object MVCC stats key.
/// Format: `[VERSION][0x25][0x02][tier:1][id:variable]`
pub fn encode_storage_stats_key(tier: Tier, id: MetricId) -> Vec<u8> {
	let mut key = vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_OBJECT, tier_to_byte(tier)];
	encode_object_id(&mut key, id);
	key
}

/// Create a prefix for scanning all per-type stats keys.
pub fn type_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_TYPE]
}

/// Create a prefix for scanning all per-object MVCC stats keys.
pub fn storage_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_BY_OBJECT]
}

// ============================================================================
// CDC Stats Key Encoding
// ============================================================================

/// Encode a CDC stats key.
/// Format: `[VERSION][0x25][0x03][id:variable]`
pub fn encode_cdc_stats_key(id: MetricId) -> Vec<u8> {
	let mut key = vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_CDC];
	encode_object_id(&mut key, id);
	key
}

/// Create a prefix for scanning all CDC stats keys.
pub fn cdc_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::Metric as u8, SUBKEY_CDC]
}

// ============================================================================
// Key Decoding
// ============================================================================

/// Decode a per-type stats key back into (Tier, KeyKind).
/// Returns None if the key is malformed or not a type stats key.
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

/// Decode a per-object MVCC stats key back into (Tier, Id).
/// Returns None if the key is malformed or not a storage stats key.
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

/// Decode a CDC stats key back into Id.
/// Returns None if the key is malformed or not a CDC stats key.
pub fn decode_cdc_stats_key(key: &[u8]) -> Option<MetricId> {
	if key.len() < 4 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::Metric as u8 || key[2] != SUBKEY_CDC {
		return None;
	}
	decode_object_id(&key[3..])
}

// ============================================================================
// Value Encoding/Decoding
// ============================================================================

/// StorageStats is 48 bytes (6 x u64).
pub const STORAGE_STATS_SIZE: usize = 48;

/// Encode StorageStats to bytes.
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

/// Decode StorageStats from bytes.
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

/// CdcStats is 24 bytes (3 x u64).
pub const CDC_STATS_SIZE: usize = 24;

/// Encode CdcStats to bytes.
pub fn encode_cdc_stats(stats: &CdcStats) -> Vec<u8> {
	let mut buf = Vec::with_capacity(CDC_STATS_SIZE);
	buf.extend_from_slice(&stats.key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.entry_count.to_le_bytes());
	buf
}

/// Decode CdcStats from bytes.
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

// ============================================================================
// Tier Encoding
// ============================================================================

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

// ============================================================================
// Id Encoding
// ============================================================================

fn encode_object_id(buf: &mut Vec<u8>, id: MetricId) {
	match id {
		MetricId::Source(source_id) => {
			buf.push(ID_SOURCE);
			buf.extend_from_slice(&encode_source_id(source_id));
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
		ID_SOURCE => {
			if bytes.len() < 10 {
				return None;
			}
			let source_id = decode_source_id(&bytes[1..10])?;
			Some(MetricId::Source(source_id))
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

// PrimitiveId encoding (9 bytes: 1 byte discriminant + 8 bytes id)
fn encode_source_id(source_id: PrimitiveId) -> [u8; 9] {
	let mut buf = [0u8; 9];
	buf[0] = source_id.to_type_u8();
	buf[1..9].copy_from_slice(&source_id.as_u64().to_be_bytes());
	buf
}

fn decode_source_id(bytes: &[u8]) -> Option<PrimitiveId> {
	if bytes.len() < 9 {
		return None;
	}
	let discriminant = bytes[0];
	let id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);

	match discriminant {
		1 => Some(PrimitiveId::Table(TableId(id))),
		2 => Some(PrimitiveId::View(ViewId(id))),
		3 => Some(PrimitiveId::Flow(FlowId(id))),
		4 => Some(PrimitiveId::TableVirtual(VTableId(id))),
		5 => Some(PrimitiveId::RingBuffer(RingBufferId(id))),
		6 => Some(PrimitiveId::Dictionary(DictionaryId(id))),
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{flow::FlowNodeId, id::TableId, primitive::PrimitiveId};

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
		let source_id = PrimitiveId::Table(TableId(12345));
		let id = MetricId::Source(source_id);

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
		let source_id = PrimitiveId::Table(TableId(12345));
		let id = MetricId::Source(source_id);

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
