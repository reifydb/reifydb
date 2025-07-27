use super::base::{Operator, OperatorContext};
use crate::delta::Delta;
use crate::expression::Expression;
use crate::flow::change::Change;
use crate::row::EncodedRow;

pub struct MapOperator {
    expressions: Vec<Expression>,
}

impl MapOperator {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self { expressions }
    }
}

impl Operator for MapOperator {
    fn apply(&mut self, change: Change, _ctx: &mut OperatorContext) -> crate::Result<Change> {
        let mut output_deltas = Vec::new();

        for delta in change.deltas {
            match delta {
                Delta::Insert { key, row } => {
                    let projected_row = self.project_row(&row)?;
                    output_deltas.push(Delta::Insert { key, row: projected_row });
                }
                Delta::Update { key, row } => {
                    let projected_row = self.project_row(&row)?;
                    output_deltas.push(Delta::Update { key, row: projected_row });
                }
                Delta::Upsert { key, row } => {
                    let projected_row = self.project_row(&row)?;
                    output_deltas.push(Delta::Upsert { key, row: projected_row });
                }
                Delta::Remove { key } => {
                    // Pass through removes unchanged
                    output_deltas.push(Delta::Remove { key });
                }
            }
        }

        Ok(Change::new(output_deltas, change.version))
    }
}

impl MapOperator {
    fn project_row(&self, row: &EncodedRow) -> crate::Result<EncodedRow> {
        // TODO: Integrate with purple's expression evaluation system
        // For now, return the original row as a placeholder
        // This should evaluate each expression against the input row
        // and construct a new row with the results
        Ok(row.clone())
    }
}
