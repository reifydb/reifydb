//! Stateless operator pattern

use std::collections::HashMap;
use crate::error::Result;
use crate::operator::{FFIOperator, FlowChange, FlowDiff};
use crate::context::OperatorContext;
use crate::builders::FlowChangeBuilder;
use reifydb_core::Row;
use reifydb_type::Value;

/// Trait for stateless operators that process rows independently
pub trait StatelessOperator: Send + Sync + 'static {
    /// Transform a single row
    fn transform(&self, row: &Row) -> Result<Option<Row>>;

    /// Optional: filter rows before transformation
    fn filter(&self, _row: &Row) -> bool {
        true // Accept all rows by default
    }

    // TODO: Metadata needs to be redesigned for static approach
}

/// Adapter that converts a StatelessOperator to a regular Operator
pub struct StatelessAdapter<T: StatelessOperator> {
    inner: T,
}

impl<T: StatelessOperator> StatelessAdapter<T> {
    /// Create a new adapter
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: StatelessOperator> FFIOperator for StatelessAdapter<T> {
    fn new() -> Self {
        // TODO: This needs redesign for the new operator model
        panic!("StatelessAdapter needs redesign for new operator model")
    }

    fn initialize(&mut self, _config: &HashMap<String, Value>) -> Result<()> {
        // TODO: Initialize the inner operator
        Ok(())
    }

    fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
        let mut builder = FlowChangeBuilder::new().with_version(input.version);

        for diff in input.diffs {
            match diff {
                FlowDiff::Insert { post } => {
                    if self.inner.filter(&post) {
                        if let Some(transformed) = self.inner.transform(&post)? {
                            builder = builder.insert(transformed);
                        }
                    }
                }
                FlowDiff::Update { pre, post } => {
                    if self.inner.filter(&post) {
                        if let Some(transformed) = self.inner.transform(&post)? {
                            builder = builder.update(pre, transformed);
                        } else {
                            // If transform returns None, remove the row
                            builder = builder.remove(pre);
                        }
                    } else {
                        // If filtered out, remove the row
                        builder = builder.remove(pre);
                    }
                }
                FlowDiff::Remove { pre } => {
                    builder = builder.remove(pre);
                }
            }
        }

        Ok(builder.build())
    }

    fn get_rows(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[reifydb_type::RowNumber]) -> Result<Vec<Option<Row>>> {
        // TODO: Implement for stateless pattern
        Ok(vec![])
    }
}

/// Helper function to create a stateless operator adapter
pub fn stateless<T: StatelessOperator>(operator: T) -> StatelessAdapter<T> {
    StatelessAdapter::new(operator)
}