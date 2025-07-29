// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::sub_cannot_be_applied_to_incompatible_types;
use reifydb_core::expression::SubExpression;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, Push};
use reifydb_core::frame::column::container::number::NumberContainer;
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeSub};
use reifydb_core::{GetType, Type, return_error};
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn sub(
        &mut self,
        sub: &SubExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&sub.left, ctx)?;
        let right = self.evaluate(&sub.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.values(), &right.values()) {
            // Float4
            (ColumnValues::Float4(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float4(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int1(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint1(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Float8
            (ColumnValues::Float8(l), ColumnValues::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Float8(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int1(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint1(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int2(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int4(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int8(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int16(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int1(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int2(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int2(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int4(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int4(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int8(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int8(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Int16(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Int16(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint2(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint4(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint8(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint16(l), ColumnValues::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint1(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint2(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint2(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint4(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint4(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint8(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint8(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (ColumnValues::Uint16(l), ColumnValues::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (ColumnValues::Uint16(l), ColumnValues::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            _ => return_error!(sub_cannot_be_applied_to_incompatible_types(
                sub.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn sub_numeric<L, R>(
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
    <L as Promote<R>>::Output: SafeSub,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());
    
    let mut values = ctx.pooled_values(target, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                if let Some(value) = ctx.sub(*l, *r, &span)? {
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
