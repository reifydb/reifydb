// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::OwnedSpan;
use reifydb_core::error::diagnostic::operator::add_cannot_be_applied_to_incompatible_types;
use reifydb_core::expression::AddExpression;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, Push};
use reifydb_core::value::IsNumber;
use reifydb_core::value::number::{Promote, SafeAdd};
use reifydb_core::{BitVec, CowVec, GetType, Type, return_error};

impl Evaluator {
    pub(crate) fn add(
        &mut self,
        add: &AddExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let left = self.evaluate(&add.left, ctx)?;
        let right = self.evaluate(&add.right, ctx)?;
        let ty = Type::promote(left.get_type(), right.get_type());

        match (&left.values(), &right.values()) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                add_numeric(ctx, l, r, lv, rv, ty, add.span())
            }

            _ => return_error!(add_cannot_be_applied_to_incompatible_types(
                add.span(),
                left.get_type(),
                right.get_type(),
            )),
        }
    }
}

fn add_numeric<L, R>(
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
    <L as Promote<R>>::Output: SafeAdd,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    debug_assert_eq!(l.len(), r.len());
    debug_assert_eq!(lv.len(), rv.len());

    let mut values = ctx.pooled_values(ty, lv.len());
    for i in 0..l.len() {
        if lv.get(i) && rv.get(i) {
            if let Some(value) = ctx.add(l[i], r[i], &span)? {
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
