// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::error::diagnostic::operator::between_cannot_be_applied_to_incompatible_types;
use reifydb_core::expression::{
    BetweenExpression, GreaterThanEqualExpression, LessThanEqualExpression,
};
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn};
use reifydb_core::return_error;

impl Evaluator {
    pub(crate) fn between(
        &mut self,
        expr: &BetweenExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        // Create temporary expressions for the comparisons
        let greater_equal_expr = GreaterThanEqualExpression {
            left: expr.value.clone(),
            right: expr.lower.clone(),
            span: expr.span.clone(),
        };

        let less_equal_expr = LessThanEqualExpression {
            left: expr.value.clone(),
            right: expr.upper.clone(),
            span: expr.span.clone(),
        };

        // Evaluate both comparisons
        let ge_result = self.greater_than_equal(&greater_equal_expr, ctx)?;
        let le_result = self.less_than_equal(&less_equal_expr, ctx)?;

        // Check that both results are boolean (they should be if the comparison succeeded)
        if !matches!(ge_result.values(), ColumnValues::Bool(_, _))
            || !matches!(le_result.values(), ColumnValues::Bool(_, _))
        {
            // This should not happen if the comparison operators work correctly,
            // but we handle it as a safety measure
            let value = self.evaluate(&expr.value, ctx)?;
            let lower = self.evaluate(&expr.lower, ctx)?;
            return return_error!(between_cannot_be_applied_to_incompatible_types(
                expr.span(),
                value.get_type(),
                lower.get_type(),
            ));
        }

        // Combine the results with AND logic
        let ge_values = ge_result.values();
        let le_values = le_result.values();

        match (ge_values, le_values) {
            (ColumnValues::Bool(ge_vals, ge_bitvec), ColumnValues::Bool(le_vals, le_bitvec)) => {
                let mut result_values = Vec::with_capacity(ge_vals.len());
                let mut result_bitvec = Vec::with_capacity(ge_bitvec.len());

                for i in 0..ge_vals.len() {
                    if ge_bitvec.get(i) && le_bitvec.get(i) {
                        result_values.push(ge_vals[i] && le_vals[i]);
                        result_bitvec.push(true);
                    } else {
                        result_values.push(false);
                        result_bitvec.push(false);
                    }
                }

                Ok(FrameColumn::ColumnQualified(ColumnQualified {
                    name: expr.span.fragment.clone(),
                    values: ColumnValues::bool_with_bitvec(result_values, result_bitvec),
                }))
            }
            _ => {
                // This should never be reached due to the check above
                unreachable!("Both comparison results should be boolean after the check above")
            }
        }
    }
}