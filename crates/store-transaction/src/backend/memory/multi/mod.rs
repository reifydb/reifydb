// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod commit;
mod contains;
mod get;
mod range;
mod range_rev;

pub use range::MultiVersionRangeIter;
pub use range_rev::MultiVersionRangeRevIter;
use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, FlowNodeId, FlowNodeStateKeyRange, Key, RowKeyRange, SourceId},
};

/// Classify a key to determine which storage it belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageType {
	Source(SourceId),
	Operator(FlowNodeId),
	Multi, // Fallback for other key types
}

pub(crate) fn classify_key(key: &EncodedKey) -> StorageType {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => StorageType::Source(row_key.source),
		Some(Key::FlowNodeState(state_key)) => StorageType::Operator(state_key.node),
		_ => StorageType::Multi,
	}
}

/// Classify a range to determine which storage it belongs to
/// Returns the storage type, or None if the range spans multiple storages
pub(crate) fn classify_range(range: &EncodedKeyRange) -> Option<StorageType> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(StorageType::Source(start.source));
	}

	if let (Some(start), Some(_end)) = FlowNodeStateKeyRange::decode(range) {
		return Some(StorageType::Operator(start.node));
	}

	None
}
