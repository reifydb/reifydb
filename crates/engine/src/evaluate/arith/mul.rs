// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{FrameColumn, ColumnValues, Push};
use reifydb_core::Span;
use reifydb_core::num::{IsNumber, Promote, SafeMul};
use reifydb_core::{CowVec, DataType, GetKind};
use reifydb_rql::expression::MulExpression;

impl Evaluator {
    pub(crate) fn mul(
		&mut self,
		mul: &MulExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let left = self.evaluate(&mul.left, ctx)?;
        let right = self.evaluate(&mul.right, ctx)?;
        let data_type = DataType::promote(left.data_type(), right.data_type());

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                mul_numeric(ctx, l, r, lv, rv, data_type, mul.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn mul_numeric<L, R>(
	ctx: &EvaluationContext,
	l: &CowVec<L>,
	r: &CowVec<R>,
	lv: &CowVec<bool>,
	rv: &CowVec<bool>,
	data_type: DataType,
	span: Span,
) -> crate::evaluate::Result<FrameColumn>
where
    L: GetKind + Promote<R> + Copy,
    R: GetKind + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeMul,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(lv.len(), rv.len());

    let mut data = ColumnValues::with_capacity(data_type, lv.len());
    for i in 0..l.len() {
        if lv[i] && rv[i] {
            if let Some(value) = ctx.mul(l[i], r[i], &span)? {
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
