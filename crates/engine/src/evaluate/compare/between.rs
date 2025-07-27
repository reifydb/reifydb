// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{Evaluator, EvaluationContext};
use reifydb_core::frame::{ColumnValues, FrameColumn, ColumnQualified};
use reifydb_rql::expression::{BetweenExpression, GreaterThanEqualExpression, LessThanEqualExpression};

impl Evaluator {
    pub(crate) fn between(
        &mut self,
        expr: &BetweenExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        // value BETWEEN lower AND upper is equivalent to value >= lower AND value <= upper
        
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
                // Fallback: create a false result
                let len = ge_values.len().min(le_values.len());
                Ok(FrameColumn::ColumnQualified(ColumnQualified {
                    name: expr.span.fragment.clone(),
                    values: ColumnValues::bool(vec![false; len]),
                }))
            }
        }
    }
}