// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::SubExpression},
	value::{
		columnar::{Column, ColumnData, ColumnQualified, push::Push},
		container::{UndefinedContainer, number::NumberContainer},
	},
};
use reifydb_type::{
	GetType, IsNumber, LazyFragment, Promote, SafeSub, Type,
	diagnostic::operator::sub_cannot_be_applied_to_incompatible_types,
	return_error,
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn sub(
		&self,
		ctx: &EvaluationContext,
		sub: &SubExpression,
	) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &sub.left)?;
		let right = self.evaluate(ctx, &sub.right)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, || {
					sub.full_fragment_owned()
				})
			}

			// Handle undefined values - any operation with
			// undefined results in undefined
			(ColumnData::Undefined(l), _) => {
				Ok(Column::ColumnQualified(ColumnQualified {
					name: sub
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
					name: sub
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
				sub_cannot_be_applied_to_incompatible_types(
					sub.full_fragment_owned(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn sub_numeric<'a, L, R>(
	ctx: &EvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment<'a> + Copy,
) -> crate::Result<Column>
where
	L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
	R: GetType + IsNumber + Copy + Clone + Debug + Default,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeSub,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) =
					ctx.sub(*l, *r, fragment)?
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
