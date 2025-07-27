// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::frame::{ColumnValues, FrameColumn, ColumnQualified};
use reifydb_core::error::diagnostic::operator::{
    or_can_not_applied_to_number, or_can_not_applied_to_text, 
    or_can_not_applied_to_temporal, or_can_not_applied_to_uuid
};
use reifydb_core::return_error;
use reifydb_core::expression::OrExpression;

use crate::evaluate::{EvaluationContext, Evaluator};

impl Evaluator {
    pub(crate) fn or(
        &mut self,
        expr: &OrExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&expr.left, ctx)?;
        let right = self.evaluate(&expr.right, ctx)?;

        match (&left.values(), &right.values()) {
            (ColumnValues::Bool(l, lv), ColumnValues::Bool(r, rv)) => {
                let mut values = Vec::with_capacity(l.len());
                let mut bitvec = Vec::with_capacity(lv.len());

                for i in 0..l.len() {
                    if lv.get(i) && rv.get(i) {
                        values.push(l[i] || r[i]);
                        bitvec.push(true);
                    } else {
                        values.push(false);
                        bitvec.push(false);
                    }
                }

                Ok(FrameColumn::ColumnQualified(ColumnQualified {
                    name: expr.span().fragment.into(),
                    values: ColumnValues::bool_with_bitvec(values, bitvec)
                }))
            }
            (l, r) => {
                if l.is_number() || r.is_number() {
                    return_error!(or_can_not_applied_to_number(expr.span()));
                } else if l.is_text() || r.is_text() {
                    return_error!(or_can_not_applied_to_text(expr.span()));
                } else if l.is_temporal() || r.is_temporal() {
                    return_error!(or_can_not_applied_to_temporal(expr.span()));
                } else {
                    return_error!(or_can_not_applied_to_uuid(expr.span()));
                }
            }
        }
    }
}

