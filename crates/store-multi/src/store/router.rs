// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::store::{EntryKind, classify_key, is_single_version_semantics_key},
};

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		key::{
			EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey,
			flow_node_state::FlowNodeStateKey,
		},
	};

	use super::*;

	// Basic smoke test - actual key encoding tests belong elsewhere
	#[test]
	fn test_classify_key_unknown() {
		let key = EncodedKey::new(vec![0u8; 10]);
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
		let key = EncodedKey::new(vec![0u8; 10]);
		assert!(!is_single_version_semantics_key(&key));
	}
}
