// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues, Push};
use reifydb_core::Span;
use reifydb_core::num::{IsNumber, Promote, SafeSubtract};
use reifydb_core::{CowVec, GetKind, Kind};
use reifydb_rql::expression::SubtractExpression;

impl Evaluator {
    pub(crate) fn sub(
        &mut self,
        sub: &SubtractExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        let left = self.evaluate(&sub.left, ctx)?;
        let right = self.evaluate(&sub.right, ctx)?;
        let kind = Kind::promote(left.kind(), right.kind());

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                sub_numeric(ctx, l, r, lv, rv, kind, sub.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn sub_numeric<L, R>(
    ctx: &Context,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    kind: Kind,
    span: Span,
) -> crate::evaluate::Result<Column>
where
    L: GetKind + Promote<R> + Copy,
    R: GetKind + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeSubtract,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(l.len(), r.len());

    let mut data = ColumnValues::with_capacity(kind, lv.len());
    for i in 0..l.len() {
        if lv[i] && rv[i] {
            if let Some(value) = ctx.sub(l[i], r[i], &span)? {
                data.push(value);
            } else {
                data.push_undefined()
            }
        } else {
            data.push_undefined()
        }
    }

    Ok(Column { name: span.fragment, values: data })
}
