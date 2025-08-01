use crate::flow::change::{Change, Diff};
use crate::flow::operators::{Operator, OperatorContext};
use reifydb_core::result::Frame;
use reifydb_rql::expression::Expression;

pub struct MapOperator {
    _expressions: Vec<Expression>,
}

impl MapOperator {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self { _expressions: expressions }
    }
}

impl Operator for MapOperator {
    fn apply(&mut self, _ctx: &mut OperatorContext, diff: Diff) -> crate::Result<Diff> {
        let mut output_changes = Vec::new();

        for change in diff.changes {
            match change {
                Change::Insert { columns } => {
                    let projected_columns = self.project_columns(&columns)?;
                    output_changes.push(Change::Insert { columns: projected_columns });
                }
                Change::Update { old, new } => {
                    let projected_columns = self.project_columns(&new)?;
                    output_changes.push(Change::Update { old, new: projected_columns });
                }
                Change::Remove { columns } => {
                    // For removes, we might need to project to maintain schema consistency
                    let projected_columns = self.project_columns(&columns)?;
                    output_changes.push(Change::Remove { columns: projected_columns });
                }
            }
        }

        Ok(Diff::new(output_changes))
    }
}

impl MapOperator {
    fn project_columns(&self, _frames: &Frame) -> crate::Result<Frame> {
        // if columns.is_empty() {
        //     return Ok(columns.clone());
        // }
        //
        // let row_count = columns.row_count();
        //
        // // Create evaluation context from input columns
        // let eval_ctx = EvaluationContext {
        //     target_column: None,
        //     column_policies: Vec::new(),
        //     columns: columns.columns.clone(),
        //     row_count,
        //     take: None,
        // };
        //
        // // Evaluate each expression to get projected columns
        // let mut projected_columns = Vec::new();
        // for expr in &self.expressions {
        //     let column = evaluate(expr, &eval_ctx)?;
        //     projected_columns.push(column);
        // }
        //
        // // Build new columns from projected columns
        // Ok(Columns::new(projected_columns))
        todo!()
    }
}
