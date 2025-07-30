// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::mul_cannot_be_applied_to_incompatible_types;
use reifydb_rql::expression::MulExpression;
use reifydb_core::frame::column::container::number::NumberContainer;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, Push};
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeMul};
use reifydb_core::{GetType, Type, return_error};
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn mul(
        &mut self,
        mul: &MulExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.values(), &right.values()) {
            // Float4
            (ColumnValues::Float4(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int1(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint1(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Float8
            (ColumnValues::Float8(l), ColumnValues::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int1(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint1(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int2(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int4(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int8(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int16(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int2(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int4(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int8(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Int16(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint2(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint4(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint8(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint16(l), ColumnValues::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint2(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint4(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint8(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnValues::Uint16(l), ColumnValues::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            _ => return_error!(mul_cannot_be_applied_to_incompatible_types(
                mul.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn mul_numeric<L, R>(
    ctx: &EvaluationContext,
    l: &NumberContainer<L>,
    r: &NumberContainer<R>,
    target: Type,
    span: OwnedSpan,
) -> crate::Result<FrameColumn>
where
    L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
    R: GetType + IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeMul,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());

    let mut values = ctx.pooled_values(target, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                if let Some(value) = ctx.mul(*l, *r, &span)? {
                    values.push(value);
                } else {
                    values.push_undefined()
                }
            }
            _ => values.push_undefined(),
        }
    }
    Ok(FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values }))
}
