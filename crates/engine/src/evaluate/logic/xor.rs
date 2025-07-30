// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{ColumnQualified, EngineColumn, EngineColumnData};
use reifydb_core::error::diagnostic::operator::{
    xor_can_not_applied_to_number, xor_can_not_applied_to_temporal, xor_can_not_applied_to_text,
    xor_can_not_applied_to_uuid,
};
use reifydb_core::return_error;
use reifydb_rql::expression::XorExpression;

use crate::evaluate::{EvaluationContext, Evaluator};

impl Evaluator {
    pub(crate) fn xor(
        &mut self,
        expr: &XorExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<EngineColumn> {
        let left = self.evaluate(&expr.left, ctx)?;
        let right = self.evaluate(&expr.right, ctx)?;

        match (&left.data(), &right.data()) {
            (EngineColumnData::Bool(l_container), EngineColumnData::Bool(r_container)) => {
                let mut data = Vec::with_capacity(l_container.data().len());
                let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

                for i in 0..l_container.data().len() {
                    if l_container.is_defined(i) && r_container.is_defined(i) {
                        data.push(l_container.data().get(i) != r_container.data().get(i));
                        bitvec.push(true);
                    } else {
                        data.push(false);
                        bitvec.push(false);
                    }
                }

                Ok(EngineColumn::ColumnQualified(ColumnQualified {
                    name: expr.span().fragment.into(),
                    data: EngineColumnData::bool_with_bitvec(data, bitvec),
                }))
            }
            (l, r) => {
                if l.is_number() || r.is_number() {
                    return_error!(xor_can_not_applied_to_number(expr.span()));
                } else if l.is_text() || r.is_text() {
                    return_error!(xor_can_not_applied_to_text(expr.span()));
                } else if l.is_temporal() || r.is_temporal() {
                    return_error!(xor_can_not_applied_to_temporal(expr.span()));
                } else {
                    return_error!(xor_can_not_applied_to_uuid(expr.span()));
                }
            }
        }
    }
}
