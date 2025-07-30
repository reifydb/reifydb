// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::push::Push;
use crate::column::{ColumnQualified, Column, ColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::mul_cannot_be_applied_to_incompatible_types;
use reifydb_core::value::container::number::NumberContainer;
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeMul};
use reifydb_core::{GetType, Type, return_error};
use reifydb_rql::expression::MulExpression;
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn mul(
        &mut self,
        mul: &MulExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.data(), &right.data()) {
            // Float4
            (ColumnData::Float4(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int1(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint1(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Float8
            (ColumnData::Float8(l), ColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int1(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint1(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Signed
            (ColumnData::Int1(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int2(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int4(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int8(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int16(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Unsigned
            (ColumnData::Int1(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int2(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int4(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int8(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Int16(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Signed
            (ColumnData::Uint1(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint2(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint4(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint8(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint16(l), ColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Unsigned
            (ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
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
) -> crate::Result<Column>
where
    L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
    R: GetType + IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeMul,
    ColumnData: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = ctx.pooled(target, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                if let Some(value) = ctx.mul(*l, *r, &span)? {
                    data.push(value);
                } else {
                    data.push_undefined()
                }
            }
            _ => data.push_undefined(),
        }
    }
    Ok(Column::ColumnQualified(ColumnQualified { name: span.fragment.into(), data }))
}
