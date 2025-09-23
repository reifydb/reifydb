use crate::{interface::SourceId, value::row::Row};

#[derive(Debug, Clone)]
pub enum FlowDiff {
	Insert {
		source: SourceId,
		post: Row,
	},
	Update {
		source: SourceId,
		pre: Row,
		post: Row,
	},
	Remove {
		source: SourceId,
		pre: Row,
	},
}

impl FlowDiff {
	pub fn source(&self) -> SourceId {
		match self {
			FlowDiff::Insert {
				source,
				..
			} => *source,
			FlowDiff::Update {
				source,
				..
			} => *source,
			FlowDiff::Remove {
				source,
				..
			} => *source,
		}
	}
}

#[derive(Debug, Clone)]
pub struct FlowChange {
	pub diffs: Vec<FlowDiff>,
}

impl FlowChange {
	pub fn new(diffs: Vec<FlowDiff>) -> Self {
		Self {
			diffs,
		}
	}
}
