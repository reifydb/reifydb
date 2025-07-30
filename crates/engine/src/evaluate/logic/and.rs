// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::result::error::diagnostic::operator::{
    and_can_not_applied_to_number, and_can_not_applied_to_temporal, and_can_not_applied_to_text,
    and_can_not_applied_to_uuid,
};
use crate::column::{ColumnQualified, ColumnData, Column};
use reifydb_core::return_error;
use reifydb_rql::expression::AndExpression;

use crate::evaluate::{EvaluationContext, Evaluator};

impl Evaluator {
    pub(crate) fn and(
        &mut self,
        expr: &AndExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let left = self.evaluate(&expr.left, ctx)?;
        let right = self.evaluate(&expr.right, ctx)?;

        match (&left.data(), &right.data()) {
            (ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
                let mut data = Vec::with_capacity(l_container.data().len());
                let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

                for i in 0..l_container.data().len() {
                    if l_container.is_defined(i) && r_container.is_defined(i) {
                        data.push(l_container.data().get(i) && r_container.data().get(i));
                        bitvec.push(true);
                    } else {
                        data.push(false);
                        bitvec.push(false);
                    }
                }

                Ok(Column::ColumnQualified(ColumnQualified {
                    name: expr.span().fragment.into(),
                    data: ColumnData::bool_with_bitvec(data, bitvec),
                }))
            }
            (l, r) => {
                if l.is_number() || r.is_number() {
                    return_error!(and_can_not_applied_to_number(expr.span()));
                } else if l.is_text() || r.is_text() {
                    return_error!(and_can_not_applied_to_text(expr.span()));
                } else if l.is_temporal() || r.is_temporal() {
                    return_error!(and_can_not_applied_to_temporal(expr.span()));
                } else {
                    return_error!(and_can_not_applied_to_uuid(expr.span()));
                }
            }
        }
    }
}
