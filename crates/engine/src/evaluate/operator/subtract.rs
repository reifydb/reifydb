// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues};
use reifydb_rql::expression::SubtractExpression;

impl Evaluator {
    pub(crate) fn subtract(
        &mut self,
        sub: &SubtractExpression,
        ctx: &Context,
        columns: &[&Column],
        row_count: usize,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&sub.left, ctx, columns, row_count)?;
        let right = self.evaluate(&sub.right, ctx, columns, row_count)?;

        match (&left, &right) {
            (ColumnValues::Float4(l_vals, l_valid), ColumnValues::Float4(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Float8(l_vals, l_valid), ColumnValues::Float8(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }
            (ColumnValues::Int2(l_vals, l_valid), ColumnValues::Int1(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i] as i16);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int1(l_vals, l_valid), ColumnValues::Int2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] as i16 - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int1(l_vals, l_valid), ColumnValues::Int1(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                    
                    
                        if let Some(value) = ctx.sub(l_vals[i], r_vals[i], &sub.span())? {
                            values.push(value);
                            valid.push(true);
                        } else {
                            values.push(0);
                            valid.push(false);
                        }
                    } else {
                        values.push(0);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int1_with_validity(values, valid))
            }
            (ColumnValues::Int2(l_vals, l_valid), ColumnValues::Int2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int4(l_vals, l_valid), ColumnValues::Int4(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int4_with_validity(values, valid))
            }
            (ColumnValues::Int8(l_vals, l_valid), ColumnValues::Int8(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int8_with_validity(values, valid))
            }
            (ColumnValues::Int16(l_vals, l_valid), ColumnValues::Int16(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int16_with_validity(values, valid))
            }
            (ColumnValues::Uint1(l_vals, l_valid), ColumnValues::Uint1(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint1_with_validity(values, valid))
            }
            (ColumnValues::Uint2(l_vals, l_valid), ColumnValues::Uint2(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint2_with_validity(values, valid))
            }
            (ColumnValues::Uint4(l_vals, l_valid), ColumnValues::Uint4(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint4_with_validity(values, valid))
            }
            (ColumnValues::Uint8(l_vals, l_valid), ColumnValues::Uint8(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint8_with_validity(values, valid))
            }
            (ColumnValues::Uint16(l_vals, l_valid), ColumnValues::Uint16(r_vals, r_valid)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if l_valid[i] && r_valid[i] {
                        values.push(l_vals[i] - r_vals[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint16_with_validity(values, valid))
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
