// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Key routing logic for the store layer.
//!
//! Determines which table a key or range belongs to based on key type.

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, FlowNodeStateKeyRange, Key, RowKeyRange},
};
use tracing::instrument;

use crate::backend::TableId;

/// Classify a key to determine which table it belongs to.
#[instrument(level = "trace", skip(key), fields(key_len = key.as_ref().len()))]
pub fn classify_key(key: &EncodedKey) -> TableId {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => TableId::Source(row_key.source),
		Some(Key::FlowNodeState(state_key)) => TableId::Operator(state_key.node),
		_ => TableId::Multi,
	}
}

/// Classify a range to determine which table it belongs to.
///
/// Returns `Some(TableId)` if the range is confined to a single table,
/// or `None` if the range spans multiple tables.
pub fn classify_range(range: &EncodedKeyRange) -> Option<TableId> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(TableId::Source(start.source));
	}

	if let (Some(start), Some(_end)) = FlowNodeStateKeyRange::decode(range) {
		return Some(TableId::Operator(start.node));
	}

	None
}

#[cfg(test)]
mod tests {
	use reifydb_core::CowVec;

	use super::*;

	// Basic smoke test - actual key encoding tests belong elsewhere
	#[test]
	fn test_classify_key_unknown() {
		let key = EncodedKey(CowVec::new(vec![0u8; 10]));
		assert!(matches!(classify_key(&key), TableId::Multi));
	}
}
