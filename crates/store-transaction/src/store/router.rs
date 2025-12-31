// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Key routing logic for the store layer.
//!
//! Determines which table a key or range belongs to based on key type.

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, FlowNodeInternalStateKeyRange, FlowNodeStateKeyRange, Key, RowKeyRange},
	key::KeyKind,
};

use crate::backend::TableId;

/// Classify a key to determine which table it belongs to.
pub fn classify_key(key: &EncodedKey) -> TableId {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => TableId::Source(row_key.primitive),
		Some(Key::FlowNodeState(state_key)) => TableId::Operator(state_key.node),
		Some(Key::FlowNodeInternalState(internal_key)) => TableId::Operator(internal_key.node),
		_ => TableId::Multi,
	}
}

/// Check if a key should maintain single-version semantics (drop old versions on write).
///
/// Flow node state keys (both public and internal) are only ever read at the latest
/// committed version, never for point-in-time queries. Keeping old versions wastes storage.
pub fn is_single_version_semantics_key(key: &EncodedKey) -> bool {
	Key::kind(key).is_some_and(|kind| matches!(kind, KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState))
}

/// Classify a range to determine which table it belongs to.
///
/// Returns `Some(TableId)` if the range is confined to a single table,
/// or `None` if the range spans multiple tables.
pub fn classify_range(range: &EncodedKeyRange) -> Option<TableId> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(TableId::Source(start.primitive));
	}

	if let (Some(start), Some(_end)) = FlowNodeStateKeyRange::decode(range) {
		return Some(TableId::Operator(start.node));
	}

	if let (Some(start), Some(_end)) = FlowNodeInternalStateKeyRange::decode(range) {
		return Some(TableId::Operator(start.node));
	}

	None
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CowVec,
		interface::{EncodableKey, FlowNodeId, FlowNodeInternalStateKey, FlowNodeStateKey},
	};

	use super::*;

	// Basic smoke test - actual key encoding tests belong elsewhere
	#[test]
	fn test_classify_key_unknown() {
		let key = EncodedKey(CowVec::new(vec![0u8; 10]));
		assert!(matches!(classify_key(&key), TableId::Multi));
	}

	#[test]
	fn test_classify_key_flow_node_state() {
		let key = FlowNodeStateKey::new(FlowNodeId(42), vec![1, 2, 3]).encode();
		assert!(matches!(classify_key(&key), TableId::Operator(FlowNodeId(42))));
	}

	#[test]
	fn test_classify_key_flow_node_internal_state() {
		let key = FlowNodeInternalStateKey::new(FlowNodeId(99), vec![4, 5, 6]).encode();
		assert!(matches!(classify_key(&key), TableId::Operator(FlowNodeId(99))));
	}

	#[test]
	fn test_is_single_version_semantics_key_flow_node_state() {
		let key = FlowNodeStateKey::new(FlowNodeId(1), vec![]).encode();
		assert!(is_single_version_semantics_key(&key));
	}

	#[test]
	fn test_is_single_version_semantics_key_flow_node_internal_state() {
		let key = FlowNodeInternalStateKey::new(FlowNodeId(1), vec![]).encode();
		assert!(is_single_version_semantics_key(&key));
	}

	#[test]
	fn test_is_single_version_semantics_key_unknown() {
		let key = EncodedKey(CowVec::new(vec![0u8; 10]));
		assert!(!is_single_version_semantics_key(&key));
	}
}
