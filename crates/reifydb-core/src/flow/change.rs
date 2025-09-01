use std::collections::HashMap;

use reifydb_type::{RowNumber, Value};
use serde::{Deserialize, Serialize};

use crate::{interface::StoreId, value::columnar::Columns};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowDiff {
	Insert {
		store: StoreId,
		row_ids: Vec<RowNumber>,
		after: Columns,
	},
	Update {
		store: StoreId,
		row_ids: Vec<RowNumber>,
		before: Columns,
		after: Columns,
	},
	Remove {
		store: StoreId,
		row_ids: Vec<RowNumber>,
		before: Columns,
	},
}

impl FlowDiff {
	pub fn store(&self) -> StoreId {
		match self {
			FlowDiff::Insert {
				store,
				..
			} => *store,
			FlowDiff::Update {
				store,
				..
			} => *store,
			FlowDiff::Remove {
				store,
				..
			} => *store,
		}
	}

	/// Validates that row_ids length matches the row count
	pub fn validate(&self) -> bool {
		match self {
			FlowDiff::Insert {
				row_ids,
				after,
				..
			} => row_ids.len() == after.row_count(),
			FlowDiff::Update {
				row_ids,
				before,
				after,
				..
			} => {
				row_ids.len() == before.row_count()
					&& row_ids.len() == after.row_count()
			}
			FlowDiff::Remove {
				row_ids,
				before,
				..
			} => row_ids.len() == before.row_count(),
		}
	}

	#[cfg(debug_assertions)]
	pub fn assert_valid(&self) {
		assert!(
			self.validate(),
			"Diff invariant violated: row_ids length must match row count"
		);
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
