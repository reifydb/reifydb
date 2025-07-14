// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{FrameColumn, ColumnValues, Push};
use reifydb_core::Span;
use reifydb_core::num::{IsNumber, Promote, SafeAdd};
use reifydb_core::{CowVec, GetDataType, DataType};
use reifydb_rql::expression::AddExpression;

impl Evaluator {
    pub(crate) fn add(
        &mut self,
        add: &AddExpression,
        ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let left = self.evaluate(&add.left, ctx)?;
        let right = self.evaluate(&add.right, ctx)?;
        let data_type = DataType::promote(left.data_type(), right.data_type());

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, data_type, add.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn add_numeric<L, R>(
    ctx: &EvaluationContext,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    data_type: DataType,
    span: Span,
) -> crate::evaluate::Result<FrameColumn>
where
    L: GetDataType + Promote<R> + Copy,
    R: GetDataType + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeAdd,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(lv.len(), rv.len());

    let mut data = ColumnValues::with_capacity(data_type, lv.len());
    for i in 0..l.len() {
        if lv[i] && rv[i] {
            if let Some(value) = ctx.add(l[i], r[i], &span)? {
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
