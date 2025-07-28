// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::frame::{FrameColumn, ColumnValues, ColumnQualified, Push};
use reifydb_core::OwnedSpan;
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{ Promote, SafeMul};
use reifydb_core::{Type, BitVec, CowVec, GetType, return_error};
use reifydb_core::expression::MulExpression;
use reifydb_core::error::diagnostic::operator::mul_cannot_be_applied_to_incompatible_types;

impl Evaluator {
    pub(crate) fn mul(
		&mut self,
		mul: &MulExpression,
		ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;
        let ty = Type::promote(left.get_type(), right.get_type());

        match (&left.values(), &right.values()) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, ty, mul.span())
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
    <L as Promote<R>>::Output: SafeMul,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(lv.len(), rv.len());

    // Use pooled allocation for better performance
    use crate::evaluate::pool::ColumnValuesExt;
    let mut data = ColumnValues::with_pooled_capacity(ty, lv.len(), &ctx.buffer_pool);
    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            if let Some(value) = ctx.mul(l[i], r[i], &span)? {
                data.push(value);
            } else {
                data.push_undefined()
            }
        } else {
            data.push_undefined()
        }
    }
    Ok(FrameColumn::ColumnQualified(ColumnQualified {
        name: span.fragment.into(),
        values: data
    }))
}
