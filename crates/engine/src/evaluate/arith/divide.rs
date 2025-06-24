// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::DivideExpression;

const EPSILON32: f32 = 1e-7;
const EPSILON64: f64 = 1e-14;

impl Evaluator {
    pub(crate) fn divide(
        &mut self,
        div: &DivideExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&div.left, ctx)?;
        let right = self.evaluate(&div.right, ctx)?;

        let row_count = ctx.row_count;
        match (&left, &right) {
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i].abs() > EPSILON32 {
                        values.push(l[i] / r[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i].abs() > EPSILON64 {
                        values.push(l[i] / r[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f64 / r[i] as f64);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                // FIXME instead of float8 it should return some Decimal / BigDecimal value

                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f64 / r[i] as f64);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f32 / r[i] as f32);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float4_with_validity(values, valid))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f64 / r[i] as f64);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::float8_with_validity(values, valid))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                // FIXME instead of float8 it should return some Decimal / BigDecimal value

                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] && r[i] != 0 {
                        values.push(l[i] as f64 / r[i] as f64);
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
