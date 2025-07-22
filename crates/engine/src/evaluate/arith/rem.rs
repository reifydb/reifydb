// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{FrameColumn, ColumnValues, Push};
use reifydb_core::OwnedSpan;
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{ Promote, SafeRemainder};
use reifydb_core::{Type, BitVec, CowVec, GetType};
use reifydb_rql::expression::RemExpression;

impl Evaluator {
    pub(crate) fn rem(
        &mut self,
        rem: &RemExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&rem.left, ctx)?;
        let right = self.evaluate(&rem.right, ctx)?;
        let ty = Type::promote(left.get_type(), right.get_type());

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                rem_numeric(ctx, l, r, lv, rv, ty, rem.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn rem_numeric<L, R>(
    ctx: &EvaluationContext,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &BitVec,
    rv: &BitVec,
    ty: Type,
    span: OwnedSpan,
) -> crate::Result<FrameColumn>
where
    L: GetType + Promote<R> + Copy,
    R: GetType + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeRemainder,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(lv.len(), rv.len());

    let mut data = ColumnValues::with_capacity(ty, lv.len());
    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            if let Some(value) = ctx.remainder(l[i], r[i], &span)? {
                data.push(value);
            } else {
                data.push_undefined()
            }
        } else {
            data.push_undefined()
        }
    }
    Ok(FrameColumn { name: span.fragment, values: data })
}
