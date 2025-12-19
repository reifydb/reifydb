// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Persistence support for storage statistics.
//!
//! Key format: All keys start with `[VERSION:1][KeyKind::StorageTracker = 0x25]`
//! followed by a sub-type discriminator:
//!
//! - `[VERSION][0x25][0x01][tier:1][key_kind:1]` -> StorageStats for (tier, KeyKind)
//! - `[VERSION][0x25][0x02][tier:1][object_id:variable]` -> StorageStats for (tier, ObjectId)

use reifydb_core::{
	interface::{FlowNodeId, SourceId},
	key::KeyKind,
};

use super::types::{ObjectId, StorageStats, Tier};

/// Current key encoding version
const KEY_VERSION: u8 = 0x01;

/// Sub-key discriminators
const SUBKEY_BY_TYPE: u8 = 0x01;
const SUBKEY_BY_OBJECT: u8 = 0x02;

/// ObjectId discriminators for encoding
const OBJECT_ID_SOURCE: u8 = 0x00;
const OBJECT_ID_FLOW_NODE: u8 = 0x01;
const OBJECT_ID_SYSTEM: u8 = 0x02;

// ============================================================================
// Key encoding
// ============================================================================

/// Encode a per-type stats key.
/// Format: `[VERSION][0x25][0x01][tier:1][key_kind:1]`
pub fn encode_type_stats_key(tier: Tier, kind: KeyKind) -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::StorageTracker as u8, SUBKEY_BY_TYPE, tier_to_byte(tier), kind as u8]
}

/// Encode a per-object stats key.
/// Format: `[VERSION][0x25][0x02][tier:1][object_id:variable]`
pub fn encode_object_stats_key(tier: Tier, object_id: ObjectId) -> Vec<u8> {
	let mut key = vec![KEY_VERSION, KeyKind::StorageTracker as u8, SUBKEY_BY_OBJECT, tier_to_byte(tier)];
	encode_object_id(&mut key, object_id);
	key
}

/// Create a prefix for scanning all per-type stats keys.
pub fn type_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::StorageTracker as u8, SUBKEY_BY_TYPE]
}

/// Create a prefix for scanning all per-object stats keys.
pub fn object_stats_key_prefix() -> Vec<u8> {
	vec![KEY_VERSION, KeyKind::StorageTracker as u8, SUBKEY_BY_OBJECT]
}

// ============================================================================
// Key decoding
// ============================================================================

/// Decode a per-type stats key back into (Tier, KeyKind).
/// Returns None if the key is malformed or not a type stats key.
pub fn decode_type_stats_key(key: &[u8]) -> Option<(Tier, KeyKind)> {
	if key.len() < 5 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::StorageTracker as u8 || key[2] != SUBKEY_BY_TYPE {
		return None;
	}
	let tier = byte_to_tier(key[3])?;
	let kind = KeyKind::try_from(key[4]).ok()?;
	Some((tier, kind))
}

/// Decode a per-object stats key back into (Tier, ObjectId).
/// Returns None if the key is malformed or not an object stats key.
pub fn decode_object_stats_key(key: &[u8]) -> Option<(Tier, ObjectId)> {
	if key.len() < 5 {
		return None;
	}
	if key[0] != KEY_VERSION || key[1] != KeyKind::StorageTracker as u8 || key[2] != SUBKEY_BY_OBJECT {
		return None;
	}
	let tier = byte_to_tier(key[3])?;
	let object_id = decode_object_id(&key[4..])?;
	Some((tier, object_id))
}

// ============================================================================
// Value encoding/decoding (StorageStats)
// ============================================================================

/// StorageStats is 48 bytes (6 x u64).
pub const STORAGE_STATS_SIZE: usize = 48;

/// Encode StorageStats to bytes.
pub fn encode_stats(stats: &StorageStats) -> Vec<u8> {
	let mut buf = Vec::with_capacity(STORAGE_STATS_SIZE);
	buf.extend_from_slice(&stats.current_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.current_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.historical_key_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.historical_value_bytes.to_le_bytes());
	buf.extend_from_slice(&stats.current_entry_count.to_le_bytes());
	buf.extend_from_slice(&stats.historical_entry_count.to_le_bytes());
	buf
}

/// Decode StorageStats from bytes.
pub fn decode_stats(bytes: &[u8]) -> Option<StorageStats> {
	if bytes.len() < STORAGE_STATS_SIZE {
		return None;
	}
	Some(StorageStats {
		current_key_bytes: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
		current_value_bytes: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
		historical_key_bytes: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
		historical_value_bytes: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
		current_entry_count: u64::from_le_bytes(bytes[32..40].try_into().ok()?),
		historical_entry_count: u64::from_le_bytes(bytes[40..48].try_into().ok()?),
	})
}

// ============================================================================
// Tier encoding
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
// ObjectId encoding
// ============================================================================

fn encode_object_id(buf: &mut Vec<u8>, object_id: ObjectId) {
	match object_id {
		ObjectId::Source(source_id) => {
			buf.push(OBJECT_ID_SOURCE);
			buf.extend_from_slice(&encode_source_id(source_id));
		}
		ObjectId::FlowNode(flow_node_id) => {
			buf.push(OBJECT_ID_FLOW_NODE);
			buf.extend_from_slice(&flow_node_id.0.to_le_bytes());
		}
		ObjectId::System => {
			buf.push(OBJECT_ID_SYSTEM);
		}
	}
}

fn decode_object_id(bytes: &[u8]) -> Option<ObjectId> {
	if bytes.is_empty() {
		return None;
	}
	match bytes[0] {
		OBJECT_ID_SOURCE => {
			if bytes.len() < 10 {
				return None;
			}
			let source_id = decode_source_id(&bytes[1..10])?;
			Some(ObjectId::Source(source_id))
		}
		OBJECT_ID_FLOW_NODE => {
			if bytes.len() < 9 {
				return None;
			}
			let id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
			Some(ObjectId::FlowNode(FlowNodeId(id)))
		}
		OBJECT_ID_SYSTEM => Some(ObjectId::System),
		_ => None,
	}
}

// SourceId encoding (9 bytes: 1 byte discriminant + 8 bytes id)
fn encode_source_id(source_id: SourceId) -> [u8; 9] {
	let mut buf = [0u8; 9];
	buf[0] = source_id.to_type_u8();
	buf[1..9].copy_from_slice(&source_id.as_u64().to_be_bytes());
	buf
}

fn decode_source_id(bytes: &[u8]) -> Option<SourceId> {
	use reifydb_core::interface::{DictionaryId, FlowId, RingBufferId, TableId, TableVirtualId, ViewId};

	if bytes.len() < 9 {
		return None;
	}
	let discriminant = bytes[0];
	let id = u64::from_be_bytes(bytes[1..9].try_into().ok()?);

	match discriminant {
		1 => Some(SourceId::Table(TableId(id))),
		2 => Some(SourceId::View(ViewId(id))),
		3 => Some(SourceId::Flow(FlowId(id))),
		4 => Some(SourceId::TableVirtual(TableVirtualId(id))),
		5 => Some(SourceId::RingBuffer(RingBufferId(id))),
		6 => Some(SourceId::Dictionary(DictionaryId(id))),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId;

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
	fn test_object_stats_key_source_roundtrip() {
		let tier = Tier::Hot;
		let source_id = SourceId::Table(TableId(12345));
		let object_id = ObjectId::Source(source_id);

		let key = encode_object_stats_key(tier, object_id);
		let decoded = decode_object_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, object_id));
	}

	#[test]
	fn test_object_stats_key_flow_node_roundtrip() {
		let tier = Tier::Cold;
		let object_id = ObjectId::FlowNode(FlowNodeId(999));

		let key = encode_object_stats_key(tier, object_id);
		let decoded = decode_object_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, object_id));
	}

	#[test]
	fn test_object_stats_key_system_roundtrip() {
		let tier = Tier::Warm;
		let object_id = ObjectId::System;

		let key = encode_object_stats_key(tier, object_id);
		let decoded = decode_object_stats_key(&key).unwrap();

		assert_eq!(decoded, (tier, object_id));
	}

	#[test]
	fn test_stats_roundtrip() {
		let stats = StorageStats {
			current_key_bytes: 100,
			current_value_bytes: 200,
			historical_key_bytes: 50,
			historical_value_bytes: 150,
			current_entry_count: 10,
			historical_entry_count: 5,
		};

		let encoded = encode_stats(&stats);
		assert_eq!(encoded.len(), STORAGE_STATS_SIZE);

		let decoded = decode_stats(&encoded).unwrap();
		assert_eq!(decoded, stats);
	}

	#[test]
	fn test_key_prefixes() {
		let type_prefix = type_stats_key_prefix();
		let object_prefix = object_stats_key_prefix();

		// Type stats key should start with type prefix
		let type_key = encode_type_stats_key(Tier::Hot, KeyKind::Row);
		assert!(type_key.starts_with(&type_prefix));

		// Object stats key should start with object prefix
		let object_key = encode_object_stats_key(Tier::Hot, ObjectId::System);
		assert!(object_key.starts_with(&object_prefix));

		// Prefixes should be different
		assert_ne!(type_prefix, object_prefix);
	}
}
