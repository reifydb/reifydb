// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues};
use reifydb_rql::expression::ModuloExpression;

impl Evaluator {
    pub(crate) fn modulo(
        &mut self,
        mo: &ModuloExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        let left = self.evaluate(&mo.left, ctx)?;
        let right = self.evaluate(&mo.right, ctx)?;

        let row_count = ctx.row_count;
        match (&left.values, &right.values) {
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f32); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::float4_with_validity(values, valid),
                })
            }

            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0.0f64); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::float8_with_validity(values, valid),
                })
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i] as i16);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int2_with_validity(values, valid),
                })
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] as i16 % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int2_with_validity(values, valid),
                })
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int1_with_validity(values, valid),
                })
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int2_with_validity(values, valid),
                })
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int4_with_validity(values, valid),
                })
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int8_with_validity(values, valid),
                })
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::int16_with_validity(values, valid),
                })
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::uint1_with_validity(values, valid),
                })
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::uint2_with_validity(values, valid),
                })
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::uint4_with_validity(values, valid),
                })
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::uint8_with_validity(values, valid),
                })
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                let mut values = Vec::with_capacity(row_count);
                let mut valid = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if lv[i] && rv[i] {
                        values.push(l[i] % r[i]);
                        valid.push(true);
                    } else {
                        values.push(0); // Placeholder
                        valid.push(false);
                    }
                }
                Ok(Column {
                    name: mo.span().fragment,
                    values: ColumnValues::uint16_with_validity(values, valid),
                })
            }
            _ => Ok(Column { name: mo.span().fragment, values: ColumnValues::Undefined(row_count) }),
        }
    }
}
