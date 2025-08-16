use std::collections::HashMap;

use reifydb_core::{Value, interface::SourceId, value::columnar::Columns};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Diff {
	Insert {
		source: SourceId,
		after: Columns,
	},
	Update {
		source: SourceId,
		before: Columns,
		after: Columns,
	},
	Remove {
		source: SourceId,
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
