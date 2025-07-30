use crate::flow::change::Diff;
use crate::flow::operators::{Operator, OperatorContext};
use reifydb_core::frame::Frame;
use reifydb_rql::expression::Expression;

pub struct FilterOperator {
    predicate: Expression,
}

impl FilterOperator {
    pub fn new(predicate: Expression) -> Self {
        Self { predicate }
    }
}

impl Operator for FilterOperator {
    fn apply(&mut self, _ctx: &mut OperatorContext, diff: Diff) -> crate::Result<Diff> {
        let mut output_changes = Vec::new();

        for change in diff.changes {
            todo!()
            // match change {
            // Change::Insert { frame } => {
            //     let filtered_frame = self.filter(&frame)?;
            //     if !filtered_frame.is_empty() {
            //         output_changes.push(Change::Insert { frame: filtered_frame });
            //     }
            // }
            // Change::Update { old, new } => {
            //     let filtered_new = self.filter(&new)?;
            //     if !filtered_new.is_empty() {
            //         output_changes.push(Change::Update { old, new: filtered_new });
            //     } else {
            //         // If new doesn't pass filter, emit remove of old
            //         output_changes.push(Change::Remove { frame: old });
            //     }
            // }
            // Change::Remove { frame } => {
            //     // Always pass through removes
            //     output_changes.push(Change::Remove { frame });
            // }
            // }
        }

        Ok(Diff::new(output_changes))
    }
}

impl FilterOperator {
    fn filter(&self, frame: &Frame) -> crate::Result<Frame> {
        // let row_count = frame.row_count();
        //
        // let eval_ctx = EvaluationContext {
        //     target_column: None,
        //     column_policies: Vec::new(),
        //     columns: frame.columns.clone(),
        //     row_count,
        //     take: None,
        // };
        //
        // // Evaluate predicate to get boolean column
        // let result_column = evaluate(&self.predicate, &eval_ctx)?;
        // let mut frame = frame.clone();
        //
        // let mut bv = BitVec::repeat(row_count, true);
        //
        // match result_column.data() {
        //     EngineColumnData::Bool(container) => {
        //         for (idx, val) in container.data().iter().enumerate() {
        //             debug_assert!(container.is_defined(idx));
        //             bv.set(idx, val);
        //         }
        //     }
        //     _ => unreachable!(),
        // }
        //
        // frame.filter(&bv)?;
        //
        // Ok(frame)
        todo!()
    }
}
