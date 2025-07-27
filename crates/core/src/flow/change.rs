use crate::delta::Delta;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Change {
    pub deltas: Vec<Delta>,
    pub metadata: HashMap<String, String>,
}

impl Change {
    pub fn new(deltas: Vec<Delta>) -> Self {
        Self { deltas, metadata: HashMap::new() }
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}
