// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Key parsing utilities for extracting ObjectId from encoded keys.

use reifydb_core::{
	interface::{DictionaryId, FlowNodeId, PrimitiveId},
	key::KeyKind,
	util::encoding::keycode::KeyDeserializer,
};

use super::types::ObjectId;

/// Extract ObjectId from an encoded key based on its KeyKind.
///
/// Different key types embed different IDs:
/// - Row, Index, IndexEntry, etc. contain PrimitiveId
/// - FlowNodeState, FlowNodeInternalState contain FlowNodeId
/// - Other keys are classified as System
pub(crate) fn extract_object_id(key: &[u8], kind: KeyKind) -> ObjectId {
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
		| KeyKind::PrimaryKey => extract_source_id(key).map(ObjectId::Source).unwrap_or(ObjectId::System),

		// Keys that contain DictionaryId at bytes 2..10
		KeyKind::DictionaryEntry | KeyKind::DictionaryEntryIndex | KeyKind::DictionarySequence => {
			extract_dictionary_id(key)
				.map(|id| ObjectId::Source(PrimitiveId::Dictionary(DictionaryId(id))))
				.unwrap_or(ObjectId::System)
		}

		// Keys that contain FlowNodeId at bytes 2..10
		KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState => {
			extract_flow_node_id(key).map(ObjectId::FlowNode).unwrap_or(ObjectId::System)
		}

		// All other key types are system metadata
		_ => ObjectId::System,
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
		key::{DictionaryEntryKey, FlowNodeStateKey, KeyKind, RowKey},
	};
	use reifydb_type::RowNumber;

	use super::*;

	#[test]
	fn test_extract_object_id_row() {
		let source = PrimitiveId::table(42);
		let row_key = RowKey {
			primitive: source,
			row: RowNumber(100),
		};
		let encoded = row_key.encode();

		let object_id = extract_object_id(encoded.as_slice(), KeyKind::Row);
		assert_eq!(object_id, ObjectId::Source(source));
	}

	#[test]
	fn test_extract_object_id_flow_node_state() {
		let node = FlowNodeId(456);
		let state_key = FlowNodeStateKey::new(node, vec![1, 2, 3]);
		let encoded = state_key.encode();

		let object_id = extract_object_id(encoded.as_slice(), KeyKind::FlowNodeState);
		assert_eq!(object_id, ObjectId::FlowNode(node));
	}

	#[test]
	fn test_extract_object_id_system() {
		// For system key kinds, should return ObjectId::System
		let fake_key = vec![0xFE, 0x01, 0, 0, 0, 0]; // Namespace kind
		let object_id = extract_object_id(&fake_key, KeyKind::Namespace);
		assert_eq!(object_id, ObjectId::System);
	}

	#[test]
	fn test_extract_object_id_dictionary() {
		let dictionary_id = DictionaryId(789);
		let hash = [0u8; 16]; // Mock hash
		let key = DictionaryEntryKey::new(dictionary_id, hash);
		let encoded = key.encode();

		let object_id = extract_object_id(encoded.as_slice(), KeyKind::DictionaryEntry);
		assert_eq!(object_id, ObjectId::Source(PrimitiveId::Dictionary(dictionary_id)));
	}
}
