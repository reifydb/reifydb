use super::base::{Operator, OperatorContext};
use crate::expression::Expression;
use crate::flow::change::{Change, Diff};
use crate::flow::row::Row;

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
        let mut output_deltas = Vec::new();

        for change in diff.changes {
            match change {
                Change::Insert { row } => {
                    // Evaluate predicate on the row
                    if self.evaluate_predicate(&row)? {
                        output_deltas.push(Change::Insert { row });
                    }
                }
                Change::Update { old, new } => {
                    // For updates, we need to check if the new row passes the filter
                    if self.evaluate_predicate(&new)? {
                        output_deltas.push(Change::Update { old, new });
                    } else {
                        // If it doesn't pass, emit a Remove
                        output_deltas.push(Change::Remove { row: old });
                    }
                }
                Change::Remove { row } => {
                    // Always pass through removes
                    output_deltas.push(Change::Remove { row });
                }
            }
        }

        Ok(Diff::new(output_deltas))
    }
}

impl FilterOperator {
    fn evaluate_predicate(&self, _row: &Row) -> crate::Result<bool> {
        // TODO: Integrate with purple's expression evaluation system
        // For now, return true as a placeholder
        // This should use purple's expression evaluation engine
        Ok(true)
    }
}
