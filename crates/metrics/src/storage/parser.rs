// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::deserializer::KeyDeserializer;
use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::{Key, catalog::KeyDeserializerCatalogExt, kind::KeyKind},
};
use reifydb_value::value::dictionary::DictionaryId;

use crate::MetricsId;

pub fn parse_id(key: &[u8]) -> MetricsId {
	let Some(kind) = Key::kind(key) else {
		return MetricsId::System;
	};
	extract_object_id(key, kind)
}

fn extract_object_id(key: &[u8], kind: KeyKind) -> MetricsId {
	match kind {
		KeyKind::Row
		| KeyKind::RowSequence
		| KeyKind::Column
		| KeyKind::Columns
		| KeyKind::ColumnSequence
		| KeyKind::ColumnProperty
		| KeyKind::Index
		| KeyKind::IndexEntry
		| KeyKind::PrimaryKey => extract_shape_id(key).map(MetricsId::Shape).unwrap_or(MetricsId::System),

		KeyKind::DictionaryEntry | KeyKind::DictionaryEntryIndex => extract_dictionary_id(key)
			.map(|id| MetricsId::Shape(ShapeId::Dictionary(DictionaryId(id))))
			.unwrap_or(MetricsId::System),

		KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState => {
			extract_flow_node_id(key).map(MetricsId::FlowNode).unwrap_or(MetricsId::System)
		}

		_ => MetricsId::System,
	}
}

fn extract_shape_id(key: &[u8]) -> Option<ShapeId> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	de.read_shape_id().ok()
}

fn extract_flow_node_id(key: &[u8]) -> Option<FlowNodeId> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	let node_id = de.read_u64().ok()?;
	Some(FlowNodeId(node_id))
}

fn extract_dictionary_id(key: &[u8]) -> Option<u64> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	de.read_u64().ok()
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{flow::FlowNodeId, shape::ShapeId},
		key::{EncodableKey, dictionary::DictionaryEntryKey, flow_node_state::FlowNodeStateKey, row::RowKey},
	};
	use reifydb_value::value::{dictionary::DictionaryId, row_number::RowNumber};

	use super::*;

	#[test]
	fn test_parse_object_id_row() {
		let shape = ShapeId::table(42);
		let encoded = RowKey::encoded(shape, RowNumber(100));

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricsId::Shape(shape));
	}

	#[test]
	fn test_parse_object_id_flow_node_state() {
		let node = FlowNodeId(456);
		let state_key = FlowNodeStateKey::new(node, vec![1, 2, 3]);
		let encoded = state_key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricsId::FlowNode(node));
	}

	#[test]
	fn test_parse_object_id_system() {
		let fake_key = vec![0xFE, 0x01, 0, 0, 0, 0];
		let id = parse_id(&fake_key);
		assert_eq!(id, MetricsId::System);
	}

	#[test]
	fn test_parse_object_id_dictionary() {
		let dictionary_id = DictionaryId(789);
		let hash = [0u8; 16];
		let key = DictionaryEntryKey::new(dictionary_id, hash);
		let encoded = key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricsId::Shape(ShapeId::Dictionary(dictionary_id)));
	}
}
