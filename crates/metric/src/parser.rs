// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Key parsing utilities for extracting Id from encoded keys.

use reifydb_core::{
	interface::{DictionaryId, FlowNodeId, PrimitiveId},
	key::{Key, KeyKind},
	util::encoding::keycode::KeyDeserializer,
};

use crate::Id;

/// Extract Id from an encoded key.
///
/// Parses the key to determine its KeyKind and then extracts the appropriate ID.
/// Returns Id::System for unrecognized keys.
pub fn parse_id(key: &[u8]) -> Id {
	let Some(kind) = Key::kind(key) else {
		return Id::System;
	};
	extract_object_id(key, kind)
}

/// Extract Id from an encoded key based on its KeyKind.
///
/// Different key types embed different IDs:
/// - Row, Index, IndexEntry, etc. contain PrimitiveId
/// - FlowNodeState, FlowNodeInternalState contain FlowNodeId
/// - Other keys are classified as System
fn extract_object_id(key: &[u8], kind: KeyKind) -> Id {
	match kind {
		// Keys that contain PrimitiveId at bytes 2..11
		KeyKind::Row
		| KeyKind::RowSequence
		| KeyKind::Column
		| KeyKind::Columns
		| KeyKind::ColumnSequence
		| KeyKind::ColumnPolicy
		| KeyKind::Index
		| KeyKind::IndexEntry
		| KeyKind::PrimaryKey => extract_source_id(key).map(Id::Source).unwrap_or(Id::System),

		// Keys that contain DictionaryId at bytes 2..10
		KeyKind::DictionaryEntry | KeyKind::DictionaryEntryIndex | KeyKind::DictionarySequence => {
			extract_dictionary_id(key)
				.map(|id| Id::Source(PrimitiveId::Dictionary(DictionaryId(id))))
				.unwrap_or(Id::System)
		}

		// Keys that contain FlowNodeId at bytes 2..10
		KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState => {
			extract_flow_node_id(key).map(Id::FlowNode).unwrap_or(Id::System)
		}

		// All other key types are system metadata
		_ => Id::System,
	}
}

/// Extract PrimitiveId from a key.
///
/// Assumes key format: `[VERSION:1][KIND:1][PrimitiveId:9][...]`
fn extract_source_id(key: &[u8]) -> Option<PrimitiveId> {
	if key.len() < 11 {
		// 1 + 1 + 9 = 11 bytes minimum
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?; // Skip version
	let _ = de.read_u8().ok()?; // Skip kind
	de.read_primitive_id().ok()
}

/// Extract FlowNodeId from a key.
///
/// Assumes key format: `[VERSION:1][KIND:1][FlowNodeId:8][...]`
fn extract_flow_node_id(key: &[u8]) -> Option<FlowNodeId> {
	if key.len() < 10 {
		// 1 + 1 + 8 = 10 bytes minimum
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?; // Skip version
	let _ = de.read_u8().ok()?; // Skip kind
	let node_id = de.read_u64().ok()?;
	Some(FlowNodeId(node_id))
}

/// Extract DictionaryId from a dictionary key.
///
/// Assumes key format: `[VERSION:1][KIND:1][DictionaryId:8][...]`
fn extract_dictionary_id(key: &[u8]) -> Option<u64> {
	if key.len() < 10 {
		// 1 + 1 + 8 = 10 bytes minimum
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?; // Skip version
	let _ = de.read_u8().ok()?; // Skip kind
	de.read_u64().ok()
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{DictionaryId, EncodableKey, FlowNodeId, PrimitiveId},
		key::{DictionaryEntryKey, FlowNodeStateKey, RowKey},
	};
	use reifydb_type::RowNumber;

	use super::*;

	#[test]
	fn test_parse_object_id_row() {
		let source = PrimitiveId::table(42);
		let encoded = RowKey::encoded(source, RowNumber(100));

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, Id::Source(source));
	}

	#[test]
	fn test_parse_object_id_flow_node_state() {
		let node = FlowNodeId(456);
		let state_key = FlowNodeStateKey::new(node, vec![1, 2, 3]);
		let encoded = state_key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, Id::FlowNode(node));
	}

	#[test]
	fn test_parse_object_id_system() {
		// For system key kinds, should return Id::System
		let fake_key = vec![0xFE, 0x01, 0, 0, 0, 0]; // Namespace kind
		let id = parse_id(&fake_key);
		assert_eq!(id, Id::System);
	}

	#[test]
	fn test_parse_object_id_dictionary() {
		let dictionary_id = DictionaryId(789);
		let hash = [0u8; 16]; // Mock hash
		let key = DictionaryEntryKey::new(dictionary_id, hash);
		let encoded = key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, Id::Source(PrimitiveId::Dictionary(dictionary_id)));
	}
}
