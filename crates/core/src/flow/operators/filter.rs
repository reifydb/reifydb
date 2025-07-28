use crate::BitVec;
use crate::expression::Expression;
use crate::flow::change::{Change, Diff};
use crate::flow::operators::{Operator, OperatorContext};
use crate::frame::Frame;

pub struct FilterOperator {
    predicate: Expression,
}

impl FilterOperator {
    pub fn new(predicate: Expression) -> Self {
        Self { predicate }
    }
}

impl Operator for FilterOperator {
    fn apply(&mut self, ctx: &mut OperatorContext, diff: Diff) -> crate::Result<Diff> {
        let mut output_changes = Vec::new();

        for change in diff.changes {
            match change {
                Change::Insert { frame } => {
                    let filtered_frame = self.filter_frame(&frame)?;
                    if !filtered_frame.is_empty() {
                        output_changes.push(Change::Insert { frame: filtered_frame });
                    }
                }
                Change::Update { old, new } => {
                    let filtered_new = self.filter_frame(&new)?;
                    if !filtered_new.is_empty() {
                        output_changes.push(Change::Update { old, new: filtered_new });
                    } else {
                        // If new doesn't pass filter, emit remove of old
                        output_changes.push(Change::Remove { frame: old });
                    }
                }
                Change::Remove { frame } => {
                    // Always pass through removes
                    output_changes.push(Change::Remove { frame });
                }
            }
        }

        Ok(Diff::new(output_changes))
    }
}

impl FilterOperator {
    fn filter_frame(&self, frame: &Frame) -> crate::Result<Frame> {
        // if frame.is_empty() {
        //     return Ok(frame.clone());
        // }
        //
        // Create evaluation context from frame
        // let eval_ctx = EvaluationContext::from_frame(frame);
        //
        // Evaluate predicate to get boolean column
        // let result_column = evaluate(&self.predicate, &eval_ctx)?;
        let mut frame = frame.clone();

        let mut bv = BitVec::new(3, true);
        bv.set(0, false);
        frame.filter(&bv).unwrap();

        dbg!(&frame);
        //
        // Filter frame using boolean mask (SIMD operation)
        // frame.filter_by_column(&result_column)
        Ok(frame)
    }
}
