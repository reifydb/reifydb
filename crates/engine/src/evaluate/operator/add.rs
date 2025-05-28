// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Evaluator;
use base::expression::AddExpression;
use frame::{Column, ColumnValues};

impl Evaluator {
    pub(crate) fn add(
        &mut self,
        add: AddExpression,
        columns: &[&Column],
        row_count: usize,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(*add.left, columns, row_count)?;
        let right = self.evaluate(*add.right, columns, row_count)?;

        match (&left, &right) {
            (ColumnValues::Int2(l_vals, l_valid), ColumnValues::Int2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] + r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }
            _ => Ok(ColumnValues::Undefined(row_count)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
