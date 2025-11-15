//! Conversion helpers between sub-flow and operator-sdk types

use reifydb_core::{CommitVersion, interface::FlowNodeId};

use crate::flow::{FlowChange, FlowChangeOrigin, FlowDiff};

/// Convert sub-flow FlowChange to operator-sdk FlowChange for marshalling
pub fn to_operator_sdk_change(change: &FlowChange) -> reifydb_flow_operator_sdk::operator::FlowChange {
	reifydb_flow_operator_sdk::operator::FlowChange {
		diffs: change.diffs.iter().map(to_operator_sdk_diff).collect(),
		version: change.version.into(),
	}
}

/// Convert sub-flow FlowDiff to operator-sdk FlowDiff
fn to_operator_sdk_diff(diff: &FlowDiff) -> reifydb_flow_operator_sdk::operator::FlowDiff {
	match diff {
		FlowDiff::Insert {
			post,
		} => reifydb_flow_operator_sdk::operator::FlowDiff::Insert {
			post: post.clone(),
		},
		FlowDiff::Update {
			pre,
			post,
		} => reifydb_flow_operator_sdk::operator::FlowDiff::Update {
			pre: pre.clone(),
			post: post.clone(),
		},
		FlowDiff::Remove {
			pre,
		} => reifydb_flow_operator_sdk::operator::FlowDiff::Remove {
			pre: pre.clone(),
		},
	}
}

/// Convert operator-sdk FlowChange back to sub-flow FlowChange after unmarshalling
pub fn from_operator_sdk_change(
	change: reifydb_flow_operator_sdk::operator::FlowChange,
	origin: FlowChangeOrigin,
) -> FlowChange {
	FlowChange {
		origin,
		diffs: change.diffs.into_iter().map(from_operator_sdk_diff).collect(),
		version: CommitVersion::from(change.version),
	}
}

/// Convert operator-sdk FlowDiff back to sub-flow FlowDiff
fn from_operator_sdk_diff(diff: reifydb_flow_operator_sdk::operator::FlowDiff) -> FlowDiff {
	match diff {
		reifydb_flow_operator_sdk::operator::FlowDiff::Insert {
			post,
		} => FlowDiff::Insert {
			post,
		},
		reifydb_flow_operator_sdk::operator::FlowDiff::Update {
			pre,
			post,
		} => FlowDiff::Update {
			pre,
			post,
		},
		reifydb_flow_operator_sdk::operator::FlowDiff::Remove {
			pre,
		} => FlowDiff::Remove {
			pre,
		},
	}
}
