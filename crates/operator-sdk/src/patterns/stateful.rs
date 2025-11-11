//! Stateful operator pattern

use crate::error::Result;
use crate::operator::{FlowChange, FlowDiff};
use crate::context::OperatorContext;
use crate::builders::FlowChangeBuilder;
use reifydb_core::Row;

/// Pattern for operators that maintain state across invocations
pub trait StatefulPattern {
    /// Initialize state if needed
    fn init_state(&mut self, _ctx: &mut OperatorContext) -> Result<()> {
        Ok(())
    }

    /// Process a row with access to state
    fn process_with_state(&mut self, ctx: &mut OperatorContext, row: &Row) -> Result<Option<Row>>;

    /// Finalize and produce any remaining output
    fn finalize(&mut self, _ctx: &mut OperatorContext) -> Result<Option<FlowChange>> {
        Ok(None)
    }
}

/// Helper to create a stateful operator
pub fn stateful_operator<F, S>(
    init: impl Fn(&mut OperatorContext) -> Result<S>,
    process: F,
) -> impl Fn(&mut OperatorContext, FlowChange) -> Result<FlowChange>
where
    F: Fn(&mut S, &mut OperatorContext, &Row) -> Result<Option<Row>>,
    S: Default,
{
    move |ctx: &mut OperatorContext, input: FlowChange| {
        // Initialize or load state
        let mut state = init(ctx)?;

        let mut builder = FlowChangeBuilder::new().with_version(input.version);

        for diff in input.diffs {
            match diff {
                FlowDiff::Insert { post } => {
                    if let Some(transformed) = process(&mut state, ctx, &post)? {
                        builder = builder.insert(transformed);
                    }
                }
                FlowDiff::Update { pre, post } => {
                    if let Some(transformed) = process(&mut state, ctx, &post)? {
                        builder = builder.update(pre, transformed);
                    } else {
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
}

/// Example: Running average operator
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct RunningAverageOperator {
        sums: HashMap<String, f64>,
        counts: HashMap<String, u64>,
    }

    impl RunningAverageOperator {
        fn new() -> Self {
            Self {
                sums: HashMap::new(),
                counts: HashMap::new(),
            }
        }

        fn update(&mut self, key: &str, value: f64) -> f64 {
            *self.sums.entry(key.to_string()).or_insert(0.0) += value;
            *self.counts.entry(key.to_string()).or_insert(0) += 1;
            self.sums[key] / self.counts[key] as f64
        }
    }

    impl StatefulPattern for RunningAverageOperator {
        fn init_state(&mut self, ctx: &mut OperatorContext) -> Result<()> {
            // Load previous state if it exists
            if let Some(sums) = ctx.state().get::<HashMap<String, f64>>("sums")? {
                self.sums = sums;
            }
            if let Some(counts) = ctx.state().get::<HashMap<String, u64>>("counts")? {
                self.counts = counts;
            }
            Ok(())
        }

        fn process_with_state(&mut self, ctx: &mut OperatorContext, _row: &Row) -> Result<Option<Row>> {
            // Update running average based on row data
            // Save state
            ctx.state().set("sums", &self.sums)?;
            ctx.state().set("counts", &self.counts)?;

            // Return transformed row
            Ok(Some(_row.clone()))
        }
    }
}