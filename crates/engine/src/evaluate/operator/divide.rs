// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::Evaluator;
use reifydb_frame::{Column, ColumnValues};
use reifydb_rql::expression::ExpressionDivide;

impl Evaluator {
    pub(crate) fn divide(
        &mut self,
        div: ExpressionDivide,
        columns: &[&Column],
        row_count: usize,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(*div.left, columns, row_count)?;
        let right = self.evaluate(*div.right, columns, row_count)?;

        match (&left, &right) {
            (ColumnValues::Int2(l_vals, l_valid), ColumnValues::Int1(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f32 / r_vals[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int1(l_vals, l_valid), ColumnValues::Int2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f32 / r_vals[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int1(l_vals, l_valid), ColumnValues::Int1(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f32 / r_vals[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Int2(l_vals, l_valid), ColumnValues::Int2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f32 / r_vals[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int4(l_vals, l_valid), ColumnValues::Int4(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f32 / r_vals[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Int8(l_vals, l_valid), ColumnValues::Int8(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f64 / r_vals[i] as f64);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }
            (ColumnValues::Int16(l_vals, l_valid), ColumnValues::Int16(r_vals, r_valid)) => {
                // FIXME instead of float8 it should return some Decimal / BigDecimal value

                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] && r_vals[i] != 0 {
                        values.push(l_vals[i] as f64 / r_vals[i] as f64);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
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
