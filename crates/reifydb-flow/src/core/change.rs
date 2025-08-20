use std::collections::HashMap;

use reifydb_catalog::row::RowId;
use reifydb_core::{Value, interface::SourceId, value::columnar::Columns};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Diff {
	Insert {
		source: SourceId,
		row_ids: Vec<RowId>,
		after: Columns,
	},
	Update {
		source: SourceId,
		row_ids: Vec<RowId>,
		before: Columns,
		after: Columns,
	},
	Remove {
		source: SourceId,
		row_ids: Vec<RowId>,
		before: Columns,
	},
}

impl Diff {
	pub fn source(&self) -> SourceId {
		match self {
			Diff::Insert {
				source,
				..
			} => *source,
			Diff::Update {
				source,
				..
			} => *source,
			Diff::Remove {
				source,
				..
			} => *source,
		}
	}

	/// Validates that row_ids length matches the row count
	pub fn validate(&self) -> bool {
		match self {
			Diff::Insert {
				row_ids,
				after,
				..
			} => row_ids.len() == after.row_count(),
			Diff::Update {
				row_ids,
				before,
				after,
				..
			} => {
				row_ids.len() == before.row_count()
					&& row_ids.len() == after.row_count()
			}
			Diff::Remove {
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
pub struct Change {
	pub diffs: Vec<Diff>,
	pub metadata: HashMap<String, Value>,
}

impl Change {
	pub fn new(diffs: Vec<Diff>) -> Self {
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
