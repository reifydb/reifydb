use crate::evaluate::pool::BufferPoolManager;
use crate::evaluate::{EvaluationContext, evaluate};
use crate::flow::change::{Change, Diff};
use crate::flow::operators::{Operator, OperatorContext};
use reifydb_core::BitVec;
use reifydb_core::expression::Expression;
use reifydb_core::frame::Frame;

pub struct MapOperator {
    expressions: Vec<Expression>,
}

impl MapOperator {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self { expressions }
    }
}

impl Operator for MapOperator {
    fn apply(&mut self, _ctx: &mut OperatorContext, diff: Diff) -> crate::Result<Diff> {
        let mut output_changes = Vec::new();

        for change in diff.changes {
            match change {
                Change::Insert { frame } => {
                    let projected_frame = self.project_frame(&frame)?;
                    output_changes.push(Change::Insert { frame: projected_frame });
                }
                Change::Update { old, new } => {
                    let projected_frame = self.project_frame(&new)?;
                    output_changes.push(Change::Update { old, new: projected_frame });
                }
                Change::Remove { frame } => {
                    // For removes, we might need to project to maintain schema consistency
                    let projected_frame = self.project_frame(&frame)?;
                    output_changes.push(Change::Remove { frame: projected_frame });
                }
            }
        }

        Ok(Diff::new(output_changes))
    }
}

impl MapOperator {
    fn project_frame(&self, frame: &Frame) -> crate::Result<Frame> {
        if frame.is_empty() {
            return Ok(frame.clone());
        }

        let row_count = frame.row_count();

        // Create evaluation context from input frame
        let eval_ctx = EvaluationContext {
            target_column: None,
            column_policies: Vec::new(),
            mask: BitVec::new(row_count, true),
            columns: frame.columns.clone(),
            row_count,
            take: None,
            buffer_pool: BufferPoolManager::default(),
        };

        // Evaluate each expression to get projected columns
        let mut projected_columns = Vec::new();
        for expr in &self.expressions {
            let column = evaluate(expr, &eval_ctx)?;
            projected_columns.push(column);
        }

        // Build new frame from projected columns
        Ok(Frame::new(projected_columns))
    }
}
