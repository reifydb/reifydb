// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::push::Push;
use crate::column::{ColumnQualified, EngineColumn, EngineColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::sub_cannot_be_applied_to_incompatible_types;
use reifydb_core::value::container::number::NumberContainer;
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeSub};
use reifydb_core::{GetType, Type, return_error};
use reifydb_rql::expression::SubExpression;
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn sub(
        &mut self,
        sub: &SubExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<EngineColumn> {
        let left = self.evaluate(&sub.left, ctx)?;
        let right = self.evaluate(&sub.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.data(), &right.data()) {
            // Float4
            (EngineColumnData::Float4(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float4(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int1(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint1(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Float8
            (EngineColumnData::Float8(l), EngineColumnData::Float4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Float8(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int1(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint1(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Float8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Signed × Signed
            (EngineColumnData::Int1(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int2(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int4(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int8(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int16(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Signed × Unsigned
            (EngineColumnData::Int1(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int1(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int2(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int2(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int4(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int4(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int8(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int8(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Int16(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Int16(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Unsigned × Signed
            (EngineColumnData::Uint1(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint2(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint4(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint8(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint16(l), EngineColumnData::Int1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Int16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            // Unsigned × Unsigned
            (EngineColumnData::Uint1(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint1(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint2(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint2(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint4(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint4(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint8(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint8(l), EngineColumnData::Uint16(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }

            (EngineColumnData::Uint16(l), EngineColumnData::Uint1(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint2(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint4(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint8(r)) => {
                sub_numeric(ctx, l, r, target, sub.span())
            }
            (EngineColumnData::Uint16(l), EngineColumnData::Uint16(r)) => {
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
) -> crate::Result<EngineColumn>
where
    L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
    R: GetType + IsNumber + Copy + Clone + Debug + Default,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeSub,
    EngineColumnData: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = ctx.pooled(target, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                if let Some(value) = ctx.sub(*l, *r, &span)? {
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
