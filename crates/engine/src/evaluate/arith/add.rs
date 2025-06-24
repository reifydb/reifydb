// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{ColumnValues, Push};
use reifydb_core::num::{IsNumber, Promote, SafeAdd};
use reifydb_core::{CowVec, GetKind, Kind};
use reifydb_diagnostic::Span;
use reifydb_rql::expression::AddExpression;

impl Evaluator {
    pub(crate) fn add(
        &mut self,
        add: &AddExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&add.left, ctx)?;
        let right = self.evaluate(&add.right, ctx)?;
        let kind = Kind::promote(left.kind(), right.kind());

        match (&left, &right) {
            // (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
            //     let mut values = Vec::with_capacity(row_count);
            //     let mut valid = Vec::with_capacity(row_count);
            //     for i in 0..row_count {
            //         if lv[i] && rv[i] {
            //             values.push(l[i] + r[i]);
            //             valid.push(true);
            //         } else {
            //             values.push(0.0f32); // Placeholder
            //             valid.push(false);
            //         }
            //     }
            //     Ok(ColumnValues::float4_with_validity(values, valid))
            // }
            //
            // (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
            //     let mut values = Vec::with_capacity(row_count);
            //     let mut valid = Vec::with_capacity(row_count);
            //     for i in 0..row_count {
            //         if lv[i] && rv[i] {
            //             values.push(l[i] + r[i]);
            //             valid.push(true);
            //         } else {
            //             values.push(0.0f64); // Placeholder
            //             valid.push(false);
            //         }
            //     }
            //     Ok(ColumnValues::float8_with_validity(values, valid))
            // }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, kind, add.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn add_numeric<L, R>(
    ctx: &Context,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    kind: Kind,
    span: Span,
) -> crate::evaluate::Result<ColumnValues>
where
    L: GetKind + Promote<R> + Copy,
    R: GetKind + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeAdd,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(l.len(), r.len());

    let mut result = ColumnValues::with_capacity(kind, lv.len());
    for i in 0..l.len() {
        if lv[i] && rv[i] {
            if let Some(value) = ctx.add(l[i], r[i], &span)? {
                result.push(value);
            } else {
                result.push_undefined()
            }
        } else {
            result.push_undefined()
        }
    }
    Ok(result)
}
