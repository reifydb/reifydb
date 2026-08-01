// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
use reifydb_codec::key::encoded::EncodedKey;
#[cfg(test)]
use reifydb_core::interface::store::{EntryKind, classify_key, is_single_version_semantics_key};

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		key::{EncodableKey, flow_node_state::FlowNodeStateKey},
	};

	use super::*;

	#[test]
	fn test_classify_key_unknown() {
		// An unrecognised prefix must fall back to the catch-all Multi table, never to a typed kind.
		let key = EncodedKey::new(vec![0u8; 10]);
		assert!(matches!(classify_key(&key), EntryKind::Multi));
	}

	#[test]
	fn test_classify_key_flow_node_state() {
		let key = FlowNodeStateKey::new(FlowNodeId(42), vec![1, 2, 3]).encode();
		assert!(matches!(classify_key(&key), EntryKind::Operator(FlowNodeId(42))));
	}

	#[test]
	fn test_is_single_version_semantics_key_flow_node_state() {
		let key = FlowNodeStateKey::new(FlowNodeId(1), vec![]).encode();
		assert!(is_single_version_semantics_key(&key));
	}

	#[test]
	fn test_is_single_version_semantics_key_unknown() {
		let key = EncodedKey::new(vec![0u8; 10]);
		assert!(!is_single_version_semantics_key(&key));
	}
}
