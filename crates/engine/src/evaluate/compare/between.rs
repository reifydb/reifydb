// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{ColumnQualified, Column, ColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::result::error::diagnostic::operator::between_cannot_be_applied_to_incompatible_types;
use reifydb_core::return_error;
use reifydb_rql::expression::{
    BetweenExpression, GreaterThanEqualExpression, LessThanEqualExpression,
};

impl Evaluator {
    pub(crate) fn between(
        &mut self,
        expr: &BetweenExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
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
        if !matches!(ge_result.data(), ColumnData::Bool(_))
            || !matches!(le_result.data(), ColumnData::Bool(_))
        {
            // This should not happen if the comparison operators work correctly,
            // but we handle it as a safety measure
            let value = self.evaluate(&expr.value, ctx)?;
            let lower = self.evaluate(&expr.lower, ctx)?;
            return_error!(between_cannot_be_applied_to_incompatible_types(
                expr.span(),
                value.get_type(),
                lower.get_type(),
            ))
        }

        // Combine the results with AND logic
        let ge_data = ge_result.data();
        let le_data = le_result.data();

        match (ge_data, le_data) {
            (ColumnData::Bool(ge_container), ColumnData::Bool(le_container)) => {
                let mut data = Vec::with_capacity(ge_container.len());
                let mut bitvec = Vec::with_capacity(ge_container.len());

                for i in 0..ge_container.len() {
                    if ge_container.is_defined(i) && le_container.is_defined(i) {
                        data.push(ge_container.data().get(i) && le_container.data().get(i));
                        bitvec.push(true);
                    } else {
                        data.push(false);
                        bitvec.push(false);
                    }
                }

                Ok(Column::ColumnQualified(ColumnQualified {
                    name: expr.span.fragment.clone(),
                    data: ColumnData::bool_with_bitvec(data, bitvec),
                }))
            }
            _ => {
                // This should never be reached due to the check above
                unreachable!("Both comparison results should be boolean after the check above")
            }
        }
    }
}
