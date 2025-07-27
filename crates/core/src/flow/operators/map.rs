use super::base::{Operator, OperatorContext};
use crate::expression::Expression;
use crate::flow::change::{Change, Diff};
use crate::flow::row::Row;

pub struct MapOperator {
    expressions: Vec<Expression>,
}

impl MapOperator {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self { expressions }
    }
}

impl Operator for MapOperator {
    fn apply(&mut self, ctx: &mut OperatorContext, diff: Diff) -> crate::Result<Diff> {
        let mut output_changes = Vec::new();

        for change in diff.changes {
            match change {
                Change::Insert { row } => {
                    let projected_row = self.project_row(&row)?;
                    output_changes.push(Change::Insert { row: projected_row });
                }
                Change::Update { old, new } => {
                    let projected_row = self.project_row(&new)?;
                    output_changes.push(Change::Update { old, new: projected_row });
                }
                Change::Remove { row } => {
                    // Pass through removes unchanged
                    output_changes.push(Change::Remove { row });
                }
            }
        }

        Ok(Diff::new(output_changes))
    }
}

impl MapOperator {
    fn project_row(&self, row: &Row) -> crate::Result<Row> {
        // TODO: Integrate with purple's expression evaluation system
        // For now, return the original row as a placeholder
        // This should evaluate each expression against the input row
        // and construct a new row with the results
        Ok(row.clone())
    }
}
