//! Stateless operator pattern

use crate::error::Result;
use crate::operator::{Operator, OperatorMetadata, FlowChange, FlowDiff};
use crate::context::OperatorContext;
use crate::builders::FlowChangeBuilder;
use reifydb_core::Row;

/// Trait for stateless operators that process rows independently
pub trait StatelessOperator: Send + Sync + 'static {
    /// Transform a single row
    fn transform(&self, row: &Row) -> Result<Option<Row>>;

    /// Optional: filter rows before transformation
    fn filter(&self, _row: &Row) -> bool {
        true // Accept all rows by default
    }

    /// Metadata for the operator
    fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata::default()
    }
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

impl<T: StatelessOperator> Operator for StatelessAdapter<T> {
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

    fn metadata(&self) -> OperatorMetadata {
        self.inner.metadata()
    }
}

/// Helper function to create a stateless operator adapter
pub fn stateless<T: StatelessOperator>(operator: T) -> StatelessAdapter<T> {
    StatelessAdapter::new(operator)
}