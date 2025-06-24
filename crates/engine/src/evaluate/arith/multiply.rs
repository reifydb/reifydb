// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::MultiplyExpression;

impl Evaluator {
    pub(crate) fn multiply(
        &mut self,
        mul: &MultiplyExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;

        let row_count = ctx.row_count;

        match (&left, &right) {
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
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
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
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
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i] as i16);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] as i16 * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int1_with_validity(values, valid))
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int2_with_validity(values, valid))
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int4_with_validity(values, valid))
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int8_with_validity(values, valid))
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::int16_with_validity(values, valid))
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint1_with_validity(values, valid))
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint2_with_validity(values, valid))
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint4_with_validity(values, valid))
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::uint8_with_validity(values, valid))
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] * r[i]);
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