// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::{Column, ColumnValues, Push};
use reifydb_core::Span;
use reifydb_core::num::{IsNumber, Promote, SafeModulo};
use reifydb_core::{CowVec, DataType, GetKind};
use reifydb_rql::expression::ModuloExpression;

impl Evaluator {
    pub(crate) fn modulo(
        &mut self,
        modulo: &ModuloExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<Column> {
        let left = self.evaluate(&modulo.left, ctx)?;
        let right = self.evaluate(&modulo.right, ctx)?;
        let data_type = DataType::promote(left.data_type(), right.data_type());

        match (&left.values, &right.values) {
            // Float4
            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float4(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            // Float8
            (ColumnValues::Float8(l, lv), ColumnValues::Float4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Float8(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Float8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            // Signed × Signed
            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            // Signed × Unsigned
            (ColumnValues::Int1(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int1(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int2(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int4(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int8(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Int16(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            // Unsigned × Signed
            (ColumnValues::Uint1(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Int1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Int16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            // Unsigned × Unsigned
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint1(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint2(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint4(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint8(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint1(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint2(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint4(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint8(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }
            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                modulo_numeric(ctx, l, r, lv, rv, data_type, modulo.span())
            }

            _ => unimplemented!(),
        }
    }
}

fn modulo_numeric<L, R>(
    ctx: &Context,
    l: &CowVec<L>,
    r: &CowVec<R>,
    lv: &CowVec<bool>,
    rv: &CowVec<bool>,
    data_type: DataType,
    span: Span,
) -> crate::evaluate::Result<Column>
where
    L: GetKind + Promote<R> + Copy,
    R: GetKind + IsNumber + Copy,
    <L as Promote<R>>::Output: IsNumber,
    <L as Promote<R>>::Output: SafeModulo,
    ColumnValues: Push<<L as Promote<R>>::Output>,
{
    assert_eq!(l.len(), r.len());
    assert_eq!(l.len(), r.len());

    let mut data = ColumnValues::with_capacity(data_type, lv.len());
    for i in 0..l.len() {
        if lv[i] && rv[i] {
            if let Some(value) = ctx.modulo(l[i], r[i], &span)? {
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
