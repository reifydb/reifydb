// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::EntryKind,
	key::{
		EncodableKeyRange, Key, flow_node_internal_state::FlowNodeInternalStateKeyRange,
		flow_node_state::FlowNodeStateKeyRange, kind::KeyKind, row::RowKeyRange,
	},
};

pub fn classify_key(key: &EncodedKey) -> EntryKind {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => EntryKind::Source(row_key.shape),
		Some(Key::FlowNodeState(state_key)) => EntryKind::Operator(state_key.node),
		Some(Key::FlowNodeInternalState(internal_key)) => EntryKind::Operator(internal_key.node),
		_ => EntryKind::Multi,
	}
}

pub fn is_single_version_semantics_key(key: &EncodedKey) -> bool {
	Key::kind(key).is_some_and(|kind| matches!(kind, KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState))
}

pub fn classify_range(range: &EncodedKeyRange) -> Option<EntryKind> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(EntryKind::Source(start.shape));
	}

	if let (Some(start), Some(_end)) = FlowNodeStateKeyRange::decode(range) {
		return Some(EntryKind::Operator(start.node));
	}

	if let (Some(start), Some(_end)) = FlowNodeInternalStateKeyRange::decode(range) {
		return Some(EntryKind::Operator(start.node));
	}

	None
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		key::{
			EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey,
			flow_node_state::FlowNodeStateKey,
		},
	};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;

	// Basic smoke test - actual key encoding tests belong elsewhere
	#[test]
	fn test_classify_key_unknown() {
		let key = EncodedKey(CowVec::new(vec![0u8; 10]));
		assert!(matches!(classify_key(&key), EntryKind::Multi));
	}

	#[test]
	fn test_classify_key_flow_node_state() {
		let key = FlowNodeStateKey::new(FlowNodeId(42), vec![1, 2, 3]).encode();
		assert!(matches!(classify_key(&key), EntryKind::Operator(FlowNodeId(42))));
	}

	#[test]
	fn test_classify_key_flow_node_internal_state() {
		let key = FlowNodeInternalStateKey::new(FlowNodeId(99), vec![4, 5, 6]).encode();
		assert!(matches!(classify_key(&key), EntryKind::Operator(FlowNodeId(99))));
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
