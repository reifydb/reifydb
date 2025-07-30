use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use reifydb_core::Frame;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    Insert { columns: Frame },
    Update { old: Frame, new: Frame },
    Remove { columns: Frame },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diff {
    pub changes: Vec<Change>,
    pub metadata: HashMap<String, String>,
}

impl Diff {
    pub fn new(changes: Vec<Change>) -> Self {
        Self { changes, metadata: HashMap::new() }
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}
