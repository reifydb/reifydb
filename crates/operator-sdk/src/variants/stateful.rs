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