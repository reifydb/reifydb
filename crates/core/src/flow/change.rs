use crate::{
	interface::{FlowNodeId, SourceId},
	value::row::Row,
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
}

impl FlowChange {
	pub fn external(source: SourceId, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::External(source),
			diffs,
		}
	}

	pub fn internal(from: FlowNodeId, diffs: Vec<FlowDiff>) -> Self {
		Self {
			origin: FlowChangeOrigin::Internal(from),
			diffs,
		}
	}
}
