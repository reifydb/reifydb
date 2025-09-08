// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::RemExpression},
	value::{
		columnar::{Column, ColumnData, ColumnQualified, push::Push},
		container::{NumberContainer, UndefinedContainer},
	},
};
use reifydb_type::{
	GetType, IsNumber, LazyFragment, Promote, SafeRemainder, Type,
	diagnostic::operator::rem_cannot_be_applied_to_incompatible_types,
	return_error,
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn rem(
		&self,
		ctx: &EvaluationContext,
		rem: &RemExpression,
	) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &rem.left)?;
		let right = self.evaluate(ctx, &rem.right)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Int with other types
			(ColumnData::Int(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Int(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(ColumnData::Int(l), ColumnData::Int1(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Int2(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Int4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Int8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Int16(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint1(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint2(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Uint16(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Float4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int(l), ColumnData::Float8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Uint with other types
			(ColumnData::Uint(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Uint(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(ColumnData::Uint(l), ColumnData::Int1(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Int2(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Int4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Int8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Int16(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint1(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint2(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Uint16(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Float4(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint(l), ColumnData::Float8(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}

			// Decimal with other types
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),

			// Standard types with Int, Uint, Decimal
			(ColumnData::Int1(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Int1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Int2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Int4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Int8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Int16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),

			(ColumnData::Uint1(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Uint1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Uint2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Uint4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Uint8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(
				ColumnData::Uint16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),

			(ColumnData::Float4(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Float4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),
			(ColumnData::Float8(l), ColumnData::Int(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint(r)) => {
				rem_numeric_clone(ctx, l, r, target, || {
					rem.full_fragment_owned()
				})
			}
			(
				ColumnData::Float8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || {
				rem.full_fragment_owned()
			}),

			// Handle undefined values - any operation with
			// undefined results in undefined
			(ColumnData::Undefined(l), _) => {
				Ok(Column::ColumnQualified(ColumnQualified {
					name: rem
						.full_fragment_owned()
						.fragment()
						.into(),
					data: ColumnData::Undefined(
						UndefinedContainer::new(
							l.len(),
						),
					),
				}))
			}
			(_, ColumnData::Undefined(r)) => {
				Ok(Column::ColumnQualified(ColumnQualified {
					name: rem
						.full_fragment_owned()
						.fragment()
						.into(),
					data: ColumnData::Undefined(
						UndefinedContainer::new(
							r.len(),
						),
					),
				}))
			}

			_ => return_error!(
				rem_cannot_be_applied_to_incompatible_types(
					rem.full_fragment_owned(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn rem_numeric<'a, L, R>(
	ctx: &EvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment<'a> + Copy,
) -> crate::Result<Column>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeRemainder,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) =
					ctx.remainder(l, r, fragment)?
				{
					data.push(value);
				} else {
					data.push_undefined()
				}
			}
			_ => data.push_undefined(),
		}
	}
	let binding = fragment.fragment();
	let fragment_text = binding.text();
	Ok(Column::ColumnQualified(ColumnQualified {
		name: fragment_text.into(),
		data,
	}))
}

fn rem_numeric_clone<'a, L, R>(
	ctx: &EvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment<'a> + Copy,
) -> crate::Result<Column>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeRemainder,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_val), Some(r_val)) => {
				let l_clone = l_val.clone();
				let r_clone = r_val.clone();
				if let Some(value) = ctx.remainder(
					&l_clone, &r_clone, fragment,
				)? {
					data.push(value);
				} else {
					data.push_undefined()
				}
			}
			_ => data.push_undefined(),
		}
	}
	let binding = fragment.fragment();
	let fragment_text = binding.text();
	Ok(Column::ColumnQualified(ColumnQualified {
		name: fragment_text.into(),
		data,
	}))
}
