// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Evaluator, evaluate};
use reifydb_core::expression::{PrefixExpression, PrefixOperator};
use frame::{Column, ColumnValues};

impl Evaluator {
    pub(crate) fn prefix(
        &mut self,
        prefix: PrefixExpression,
        columns: &[&Column],
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        let values = evaluate(*prefix.expression, columns, row_count)?;

        match values {
            ColumnValues::Int2(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus => -*val,
                            PrefixOperator::Plus => *val,
                        });
                    } else {
                        result.push(0); // placeholder; will be marked invalid
                    }
                }
                Ok(ColumnValues::int2_with_validity(result, valid))
            }
            ColumnValues::Float8(_, _) => unimplemented!(),
            ColumnValues::Text(_, _) => unimplemented!(),
            ColumnValues::Bool(_, _) => unimplemented!(),
            ColumnValues::Undefined(_) => unimplemented!(),
        }
    }
}
