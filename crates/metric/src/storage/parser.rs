// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::{Key, kind::KeyKind},
	util::encoding::keycode::deserializer::KeyDeserializer,
};
use reifydb_type::value::dictionary::DictionaryId;

use crate::MetricId;

pub fn parse_id(key: &[u8]) -> MetricId {
	let Some(kind) = Key::kind(key) else {
		return MetricId::System;
	};
	extract_object_id(key, kind)
}

fn extract_object_id(key: &[u8], kind: KeyKind) -> MetricId {
	match kind {
		KeyKind::Row
		| KeyKind::RowSequence
		| KeyKind::Column
		| KeyKind::Columns
		| KeyKind::ColumnSequence
		| KeyKind::ColumnProperty
		| KeyKind::Index
		| KeyKind::IndexEntry
		| KeyKind::PrimaryKey => extract_shape_id(key).map(MetricId::Shape).unwrap_or(MetricId::System),

		KeyKind::DictionaryEntry | KeyKind::DictionaryEntryIndex | KeyKind::DictionarySequence => {
			extract_dictionary_id(key)
				.map(|id| MetricId::Shape(ShapeId::Dictionary(DictionaryId(id))))
				.unwrap_or(MetricId::System)
		}

		KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState => {
			extract_flow_node_id(key).map(MetricId::FlowNode).unwrap_or(MetricId::System)
		}

		_ => MetricId::System,
	}
}

fn extract_shape_id(key: &[u8]) -> Option<ShapeId> {
	if key.len() < 11 {
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	let _ = de.read_u8().ok()?;
	de.read_shape_id().ok()
}

fn extract_flow_node_id(key: &[u8]) -> Option<FlowNodeId> {
	if key.len() < 10 {
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	let _ = de.read_u8().ok()?;
	let node_id = de.read_u64().ok()?;
	Some(FlowNodeId(node_id))
}

fn extract_dictionary_id(key: &[u8]) -> Option<u64> {
	if key.len() < 10 {
		return None;
	}

	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	let _ = de.read_u8().ok()?;
	de.read_u64().ok()
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{flow::FlowNodeId, shape::ShapeId},
		key::{EncodableKey, dictionary::DictionaryEntryKey, flow_node_state::FlowNodeStateKey, row::RowKey},
	};
	use reifydb_type::value::{dictionary::DictionaryId, row_number::RowNumber};

	use super::*;

	#[test]
	fn test_parse_object_id_row() {
		let shape = ShapeId::table(42);
		let encoded = RowKey::encoded(shape, RowNumber(100));

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricId::Shape(shape));
	}

	#[test]
	fn test_parse_object_id_flow_node_state() {
		let node = FlowNodeId(456);
		let state_key = FlowNodeStateKey::new(node, vec![1, 2, 3]);
		let encoded = state_key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricId::FlowNode(node));
	}

	#[test]
	fn test_parse_object_id_system() {
		// For system key kinds, should return Id::System
		let fake_key = vec![0xFE, 0x01, 0, 0, 0, 0]; // Namespace kind
		let id = parse_id(&fake_key);
		assert_eq!(id, MetricId::System);
	}

	#[test]
	fn test_parse_object_id_dictionary() {
		let dictionary_id = DictionaryId(789);
		let hash = [0u8; 16]; // Mock hash
		let key = DictionaryEntryKey::new(dictionary_id, hash);
		let encoded = key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricId::Shape(ShapeId::Dictionary(dictionary_id)));
	}
}
