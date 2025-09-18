use reifydb_type::RowNumber;
use serde::{Deserialize, Serialize};

use crate::{interface::SourceId, util::CowVec, value::columnar::Columns};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowDiff {
	Insert {
		source: SourceId,
		rows: CowVec<RowNumber>,
		after: Columns,
	},
	Update {
		source: SourceId,
		rows: CowVec<RowNumber>,
		before: Columns,
		after: Columns,
	},
	Remove {
		source: SourceId,
		rows: CowVec<RowNumber>,
		before: Columns,
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

	/// Validates that row_ids length matches the row count
	pub fn validate(&self) -> bool {
		match self {
			FlowDiff::Insert {
				rows: row_ids,
				after,
				..
			} => row_ids.len() == after.row_count(),
			FlowDiff::Update {
				rows: row_ids,
				before,
				after,
				..
			} => row_ids.len() == before.row_count() && row_ids.len() == after.row_count(),
			FlowDiff::Remove {
				rows: row_ids,
				before,
				..
			} => row_ids.len() == before.row_count(),
		}
	}

	#[cfg(debug_assertions)]
	pub fn assert_valid(&self) {
		assert!(self.validate(), "Diff invariant violated: row_ids length must match row count");
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
