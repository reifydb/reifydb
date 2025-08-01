// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::push::Push;
use crate::columnar::{Column, ColumnData, ColumnQualified};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::result::error::diagnostic::operator::div_cannot_be_applied_to_incompatible_types;
use reifydb_core::value::IsNumber;
use reifydb_core::value::container::number::NumberContainer;
use reifydb_core::value::number::{Promote, SafeDiv};
use reifydb_core::{GetType, Type, return_error};
use reifydb_rql::expression::DivExpression;
use std::fmt::Debug;

impl Evaluator {
    pub(crate) fn div(
        &mut self,
        div: &DivExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let left = self.evaluate(&div.left, ctx)?;
        let right = self.evaluate(&div.right, ctx)?;
        let target = Type::promote(left.get_type(), right.get_type());

        match (&left.data(), &right.data()) {
            // Float4
            (ColumnData::Float4(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float4(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int1(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint1(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            // Float8
            (ColumnData::Float8(l), ColumnData::Float4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Float8(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int1(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint1(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Float8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            // Signed × Signed
            (ColumnData::Int1(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int2(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int4(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int8(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int16(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            // Signed × Unsigned
            (ColumnData::Int1(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int1(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int2(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int2(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int4(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int4(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int8(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int8(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Int16(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Int16(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            // Unsigned × Signed
            (ColumnData::Uint1(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint2(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint4(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint8(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint16(l), ColumnData::Int1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Int16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            // Unsigned × Unsigned
            (ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            (ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }
            (ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
                div_numeric(ctx, l, r, target, div.span())
            }

            _ => return_error!(div_cannot_be_applied_to_incompatible_types(
                div.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn div_numeric<L, R>(
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
    <L as Promote<R>>::Output: SafeDiv,
    ColumnData: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());

    let mut data = ctx.pooled(target, l.len());
    for i in 0..l.len() {
        match (l.get(i), r.get(i)) {
            (Some(l), Some(r)) => {
                if let Some(value) = ctx.div(*l, *r, &span)? {
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
