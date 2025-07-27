use crate::delta::Delta;
use std::collections::HashMap;
use uuid::Version;

#[derive(Debug, Clone)]
pub struct Change {
    pub deltas: Vec<Delta>,
    pub version: Version,
    pub metadata: HashMap<String, String>,
}

impl Change {
    pub fn new(deltas: Vec<Delta>, version: Version) -> Self {
        Self { deltas, version, metadata: HashMap::new() }
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}
