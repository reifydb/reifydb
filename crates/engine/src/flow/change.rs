use reifydb_core::frame::Frame;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Change {
    Insert { frame: Frame },
    Update { old: Frame, new: Frame },
    Remove { frame: Frame },
}

#[derive(Debug, Clone)]
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
