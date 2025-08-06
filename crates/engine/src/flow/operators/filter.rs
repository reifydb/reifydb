use crate::columnar::{ColumnData, Columns};
use crate::evaluate::{EvaluationContext, evaluate};
use crate::execute::params::ParamContext;
use crate::flow::change::{Change, Diff};
use crate::flow::operators::{Operator, OperatorContext};
use reifydb_core::BitVec;
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
            match change {
                Change::Insert { columns } => {
                    let filtered_columns = self.filter(&columns)?;
                    if !filtered_columns.is_empty() {
                        output_changes.push(Change::Insert { columns: filtered_columns });
                    }
                }
                Change::Update { old, new } => {
                    let filtered_new = self.filter(&new)?;
                    if !filtered_new.is_empty() {
                        output_changes.push(Change::Update { old, new: filtered_new });
                    } else {
                        // If new doesn't pass filter, emit remove of old
                        output_changes.push(Change::Remove { columns: old });
                    }
                }
                Change::Remove { columns } => {
                    // Always pass through removes
                    output_changes.push(Change::Remove { columns });
                }
            }
        }

        Ok(Diff::new(output_changes))
    }
}

impl FilterOperator {
    fn filter(&self, columns: &Columns) -> crate::Result<Columns> {
        let row_count = columns.row_count();

        // TODO: Flow operators need access to params through OperatorContext
        let empty_params = ParamContext::empty();
        let eval_ctx = EvaluationContext {
            target_column: None,
            column_policies: Vec::new(),
            columns: columns.clone(),
            row_count,
            take: None,
            params: &empty_params,
        };

        // Evaluate predicate to get boolean column
        let result_column = evaluate(&self.predicate, &eval_ctx)?;
        let mut columns = columns.clone();

        let mut bv = BitVec::repeat(row_count, true);

        match result_column.data() {
            ColumnData::Bool(container) => {
                for (idx, val) in container.data().iter().enumerate() {
                    debug_assert!(container.is_defined(idx));
                    bv.set(idx, val);
                }
            }
            _ => unreachable!(),
        }

        columns.filter(&bv)?;

        Ok(columns)
    }
}
