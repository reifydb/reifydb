//! Builder patterns for constructing flow changes and rows

use crate::operator::{FlowChange, FlowDiff};
use reifydb_core::{Row, CowVec, value::encoded::{EncodedValues, EncodedValuesNamedLayout}};
use reifydb_type::{RowNumber, Type, Value};
use std::collections::HashMap;

/// Builder for constructing FlowChange instances
#[derive(Default)]
pub struct FlowChangeBuilder {
    diffs: Vec<FlowDiff>,
    version: u64,
}

impl FlowChangeBuilder {
    /// Create a new FlowChangeBuilder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an insert diff
    pub fn insert(mut self, row: Row) -> Self {
        self.diffs.push(FlowDiff::Insert { post: row });
        self
    }

    /// Add an update diff
    pub fn update(mut self, pre: Row, post: Row) -> Self {
        self.diffs.push(FlowDiff::Update { pre, post });
        self
    }

    /// Add a remove diff
    pub fn remove(mut self, row: Row) -> Self {
        self.diffs.push(FlowDiff::Remove { pre: row });
        self
    }

    /// Add a diff directly
    pub fn diff(mut self, diff: FlowDiff) -> Self {
        self.diffs.push(diff);
        self
    }

    /// Add multiple diffs
    pub fn diffs(mut self, diffs: impl IntoIterator<Item = FlowDiff>) -> Self {
        self.diffs.extend(diffs);
        self
    }

    /// Set the version
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }

    /// Build the FlowChange
    pub fn build(self) -> FlowChange {
        FlowChange {
            diffs: self.diffs,
            version: self.version,
        }
    }

    /// Create an empty FlowChange
    pub fn empty() -> FlowChange {
        FlowChange {
            diffs: Vec::new(),
            version: 0,
        }
    }
}

/// Builder for constructing Row instances
pub struct RowBuilder {
    number: RowNumber,
    fields: Vec<(String, Value)>,
}

impl RowBuilder {
    /// Create a new RowBuilder
    pub fn new(number: impl Into<RowNumber>) -> Self {
        Self {
            number: number.into(),
            fields: Vec::new(),
        }
    }

    /// Add a field with a value
    pub fn field(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }

    /// Add an undefined field
    pub fn undefined_field(mut self, name: impl Into<String>) -> Self {
        self.fields.push((name.into(), Value::Undefined));
        self
    }

    /// Add fields from a map
    pub fn fields_from_map(mut self, map: HashMap<String, Value>) -> Self {
        self.fields.extend(map);
        self
    }

    /// Build the Row
    pub fn build(self) -> Row {
        // Create layout from fields
        let layout_fields: Vec<(String, Type)> = self.fields
            .iter()
            .map(|(name, value)| (name.clone(), value.get_type()))
            .collect();

        let layout = EncodedValuesNamedLayout::new(layout_fields.into_iter());

        // Encode values
        // let mut encoded_bytes = Vec::new();
        // for (_, value) in &self.fields {
        //     // Simple encoding: serialize the value as JSON then to bytes
        //     // In production, this would use the proper encoding format
        //     let json_str = serde_json::to_string(value).unwrap_or_default();
        //     let bytes = json_str.as_bytes();
        //     encoded_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        //     encoded_bytes.extend_from_slice(bytes);
        // }

        todo!();

        // Row {
        //     number: self.number,
        //     encoded: EncodedValues(CowVec::new(encoded_bytes)),
        //     layout,
        // }
    }
}

/// Extension trait for FlowChange manipulation
pub trait FlowChangeExt {
    /// Filter rows based on a predicate
    fn filter_rows<F>(&self, predicate: F) -> FlowChange
    where
        F: Fn(&Row) -> bool;

    /// Map rows through a transformation
    fn map_rows<F>(&self, f: F) -> FlowChange
    where
        F: Fn(&Row) -> Row;

    /// Count the number of diffs
    fn diff_count(&self) -> usize;

    /// Check if empty
    fn is_empty(&self) -> bool;
}

impl FlowChangeExt for FlowChange {
    fn filter_rows<F>(&self, predicate: F) -> FlowChange
    where
        F: Fn(&Row) -> bool,
    {
        let mut builder = FlowChangeBuilder::new().with_version(self.version);

        for diff in &self.diffs {
            match diff {
                FlowDiff::Insert { post } => {
                    if predicate(post) {
                        builder = builder.insert(post.clone());
                    }
                }
                FlowDiff::Update { pre, post } => {
                    if predicate(post) {
                        builder = builder.update(pre.clone(), post.clone());
                    }
                }
                FlowDiff::Remove { pre } => {
                    if predicate(pre) {
                        builder = builder.remove(pre.clone());
                    }
                }
            }
        }

        builder.build()
    }

    fn map_rows<F>(&self, f: F) -> FlowChange
    where
        F: Fn(&Row) -> Row,
    {
        let mut builder = FlowChangeBuilder::new().with_version(self.version);

        for diff in &self.diffs {
            match diff {
                FlowDiff::Insert { post } => {
                    builder = builder.insert(f(post));
                }
                FlowDiff::Update { pre, post } => {
                    builder = builder.update(f(pre), f(post));
                }
                FlowDiff::Remove { pre } => {
                    builder = builder.remove(f(pre));
                }
            }
        }

        builder.build()
    }

    fn diff_count(&self) -> usize {
        self.diffs.len()
    }

    fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
}

/// Helper to create a FlowChange with a single insert
pub fn insert_change(row: Row) -> FlowChange {
    FlowChangeBuilder::new().insert(row).build()
}

/// Helper to create a FlowChange with a single update
pub fn update_change(pre: Row, post: Row) -> FlowChange {
    FlowChangeBuilder::new().update(pre, post).build()
}

/// Helper to create a FlowChange with a single remove
pub fn remove_change(row: Row) -> FlowChange {
    FlowChangeBuilder::new().remove(row).build()
}