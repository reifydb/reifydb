// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::container::Push;
use crate::column::{ColumnQualified, EngineColumn, EngineColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::mul_cannot_be_applied_to_incompatible_types;
use crate::column::container::number::NumberContainer;
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
    ) -> crate::Result<EngineColumn> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.data(), &right.data()) {
            // Float4
            (EngineColumnData::Float4(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int1(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint1(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Float8
            (EngineColumnData::Float8(l), EngineColumnData::Float4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int1(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint1(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Float8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Signed
            (EngineColumnData::Int1(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int2(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int4(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int8(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int16(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Signed × Unsigned
            (EngineColumnData::Int1(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int2(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int4(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int8(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Int16(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Signed
            (EngineColumnData::Uint1(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint2(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint4(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint8(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint16(l), EngineColumnData::Int1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            // Unsigned × Unsigned
            (EngineColumnData::Uint1(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint2(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint4(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint8(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint16(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }

            (EngineColumnData::Uint16(l), EngineColumnData::Uint1(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint2(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint4(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint8(r)) => {
                mul_numeric(ctx, l, r, target, mul.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint16(r)) => {
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
) -> crate::Result<EngineColumn>
where
    L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
    R: GetType + IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeMul,
    EngineColumnData: Push<<L as Promote<R>>::Output>,
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
    Ok(EngineColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), data }))
}
