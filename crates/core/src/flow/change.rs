use reifydb_type::RowNumber;

use crate::{interface::SourceId, util::CowVec, value::columnar::Columns};

#[derive(Debug, Clone)]
pub enum FlowDiff {
	Insert {
		source: SourceId,
		rows: CowVec<RowNumber>,
		post: Columns<'static>,
	},
	Update {
		source: SourceId,
		rows: CowVec<RowNumber>,
		pre: Columns<'static>,
		post: Columns<'static>,
	},
	Remove {
		source: SourceId,
		rows: CowVec<RowNumber>,
		pre: Columns<'static>,
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
				rows,
				post,
				..
			} => rows.len() == post.row_count(),
			FlowDiff::Update {
				rows,
				pre,
				post,
				..
			} => rows.len() == pre.row_count() && rows.len() == post.row_count(),
			FlowDiff::Remove {
				rows,
				pre,
				..
			} => rows.len() == pre.row_count(),
		}
	}

	#[cfg(debug_assertions)]
	pub fn assert_valid(&self) {
		assert!(self.validate(), "Diff invariant violated: row_ids length must match row count");
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
