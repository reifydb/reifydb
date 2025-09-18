use std::collections::HashMap;

use reifydb_type::{RowNumber, Value};
use serde::{Deserialize, Serialize};

use crate::{interface::SourceId, value::columnar::Columns};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowDiff {
	Insert {
		source: SourceId,
		row_ids: Vec<RowNumber>,
		post: Columns,
	},
	Update {
		source: SourceId,
		row_ids: Vec<RowNumber>,
		pre: Columns,
		post: Columns,
	},
	Remove {
		source: SourceId,
		row_ids: Vec<RowNumber>,
		pre: Columns,
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
				row_ids,
				post,
				..
			} => row_ids.len() == post.row_count(),
			FlowDiff::Update {
				row_ids,
				pre,
				post,
				..
			} => row_ids.len() == pre.row_count() && row_ids.len() == post.row_count(),
			FlowDiff::Remove {
				row_ids,
				pre,
				..
			} => row_ids.len() == pre.row_count(),
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
	pub metadata: HashMap<String, Value>,
}

impl FlowChange {
	pub fn new(diffs: Vec<FlowDiff>) -> Self {
		Self {
			diffs,
			metadata: HashMap::new(),
		}
	}

	pub fn with_metadata(mut self, key: String, value: Value) -> Self {
		self.metadata.insert(key, value);
		self
	}
}
