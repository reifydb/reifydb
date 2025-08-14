use std::collections::HashMap;

use serde::{Deserialize, Serialize};

type Columns = (); // FIXME replace me

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Diff {
	Insert {
		after: Columns,
	},
	Update {
		before: Columns,
		after: Columns,
	},
	Remove {
		before: Columns,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
	pub diffs: Vec<Diff>,
	pub metadata: HashMap<String, String>,
}

impl Change {
	pub fn new(diffs: Vec<Diff>) -> Self {
		Self {
			diffs,
			metadata: HashMap::new(),
		}
	}

	pub fn with_metadata(mut self, key: String, value: String) -> Self {
		self.metadata.insert(key, value);
		self
	}
}
