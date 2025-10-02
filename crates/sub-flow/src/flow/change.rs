use reifydb_core::{
	CommitVersion, Row,
	interface::{FlowNodeId, SourceId},
};

#[derive(Debug, Clone)]
pub enum FlowChangeOrigin {
	External(SourceId),
	Internal(FlowNodeId),
}

#[derive(Debug, Clone)]
pub enum FlowDiff {
	Insert {
		post: Row,
	},
	Update {
		pre: Row,
		post: Row,
	},
	Remove {
		pre: Row,
	},
}

#[derive(Debug, Clone)]
pub struct FlowChange {
	pub origin: FlowChangeOrigin,
	pub diffs: Vec<FlowDiff>,
	pub version: CommitVersion,
}

impl FlowChange {
	pub fn external(source: SourceId, version: CommitVersion, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::External(source),
			diffs,
			version,
		}
	}

	pub fn internal(from: FlowNodeId, version: CommitVersion, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::Internal(from),
			diffs,
			version,
		}
	}
}
