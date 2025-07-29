// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::sub_cannot_be_applied_to_incompatible_types;
use reifydb_core::expression::SubExpression;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, Push};
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeSub};
use reifydb_core::{BitVec, CowVec, GetType, Type, return_error};

impl Evaluator {
    pub(crate) fn sub(
        &mut self,
        sub: &SubExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&sub.left, ctx)?;
        let right = self.evaluate(&sub.right, ctx)?;
        let ty = Type::promote(left.get_type(), right.get_type());

        match (&left.values(), &right.values()) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, ty, sub.span())
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
    <L as Promote<R>>::Output: SafeSub,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(lv.len(), rv.len());

    let mut values = ctx.pooled_values(ty, lv.len());
    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            if let Some(value) = ctx.sub(l[i], r[i], &span)? {
                values.push(value);
            } else {
                values.push_undefined()
            }
        } else {
            values.push_undefined()
        }
    }

    Ok(FrameColumn::ColumnQualified(ColumnQualified { name: span.fragment.into(), values }))
}
