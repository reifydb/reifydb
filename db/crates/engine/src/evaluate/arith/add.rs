// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::AddExpression},
	value::{
		columnar::{Column, ColumnData, ColumnQualified, push::Push},
		container::{
			NumberContainer, UndefinedContainer, Utf8Container,
		},
	},
};
use reifydb_type::{
	Fragment, GetType, IsNumber, LazyFragment, Promote, SafeAdd, Type,
	diagnostic::operator::add_cannot_be_applied_to_incompatible_types,
	return_error,
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn add(
		&self,
		ctx: &EvaluationContext,
		add: &AddExpression,
	) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &add.left)?;
		let right = self.evaluate(ctx, &add.right)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, || {
					add.full_fragment_owned()
				})
			}

			// Int operations
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int with standard numeric types
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),

			// Uint operations
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint with standard numeric types
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),

			// Decimal operations
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
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
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Decimal with standard numeric types
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),

			// Reverse operations for standard types with Int,
			// Uint, Decimal Float4 with Int, Uint,
			// Decimal
			(
				ColumnData::Float4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Float4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Float4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Float8 with Int, Uint, Decimal
			(
				ColumnData::Float8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Float8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Float8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int1 with Int, Uint, Decimal
			(
				ColumnData::Int1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int2 with Int, Uint, Decimal
			(
				ColumnData::Int2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int4 with Int, Uint, Decimal
			(
				ColumnData::Int4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int8 with Int, Uint, Decimal
			(
				ColumnData::Int8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Int16 with Int, Uint, Decimal
			(
				ColumnData::Int16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Int16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint1 with Int, Uint, Decimal
			(
				ColumnData::Uint1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint2 with Int, Uint, Decimal
			(
				ColumnData::Uint2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint4 with Int, Uint, Decimal
			(
				ColumnData::Uint4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint8 with Int, Uint, Decimal
			(
				ColumnData::Uint8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			// Uint16 with Int, Uint, Decimal
			(
				ColumnData::Uint16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),
			(
				ColumnData::Uint16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => add_numeric_clone(ctx, l, r, target, || {
				add.full_fragment_owned()
			}),

			// String concatenation
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				ColumnData::Utf8 {
					container: r,
					..
				},
			) => concat_strings(
				ctx,
				l,
				r,
				target,
				add.full_fragment_owned(),
			),

			// String + Other types (auto-promote to string)
			(
				ColumnData::Utf8 {
					container: l,
					..
				},
				r,
			) if can_promote_to_string(r) => concat_string_with_other(
				ctx,
				l,
				r,
				true,
				target,
				add.full_fragment_owned(),
			),

			// Other types + String (auto-promote to string)
			(
				l,
				ColumnData::Utf8 {
					container: r,
					..
				},
			) if can_promote_to_string(l) => concat_string_with_other(
				ctx,
				r,
				l,
				false,
				target,
				add.full_fragment_owned(),
			),

			// Handle undefined values - any operation with
			// undefined results in undefined
			(ColumnData::Undefined(l), _) => {
				Ok(Column::ColumnQualified(ColumnQualified {
					name: add
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
					name: add
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
				add_cannot_be_applied_to_incompatible_types(
					&add.full_fragment_owned(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn add_numeric<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	// Fast path: when both inputs are fully defined
	// We still need to handle potential overflow (which produces undefined
	// with Undefined policy)
	if l.is_fully_defined() && r.is_fully_defined() {
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		// Even with fully-defined inputs, operations can produce
		// undefined values due to overflow (with Undefined policy) or
		// other errors
		for i in 0..l.len() {
			// Safe to index directly since we know all values are
			// defined
			if let Some(value) =
				ctx.add(&l_data[i], &r_data[i], fragment)?
			{
				data.push(value);
			} else {
				// Overflow with Undefined policy produces
				// undefined
				data.push_undefined()
			}
		}

		let binding = fragment.fragment();
		let fragment_text = binding.text();
		return Ok(Column::ColumnQualified(ColumnQualified {
			name: fragment_text.into(),
			data,
		}));
	}

	// Slow path: some input values may be undefined
	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) = ctx.add(l, r, fragment)? {
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

fn add_numeric_clone<'a, L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_val), Some(r_val)) => {
				let l_clone = l_val.clone();
				let r_clone = r_val.clone();
				if let Some(value) =
					ctx.add(&l_clone, &r_clone, fragment)?
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

fn can_promote_to_string(data: &ColumnData) -> bool {
	matches!(
		data,
		ColumnData::Bool(_)
			| ColumnData::Float4(_)
			| ColumnData::Float8(_)
			| ColumnData::Int1(_) | ColumnData::Int2(_)
			| ColumnData::Int4(_) | ColumnData::Int8(_)
			| ColumnData::Int16(_)
			| ColumnData::Uint1(_)
			| ColumnData::Uint2(_)
			| ColumnData::Uint4(_)
			| ColumnData::Uint8(_)
			| ColumnData::Uint16(_)
			| ColumnData::Date(_) | ColumnData::DateTime(_)
			| ColumnData::Time(_) | ColumnData::Interval(_)
			| ColumnData::Uuid4(_)
			| ColumnData::Uuid7(_)
			| ColumnData::Blob { .. }
			| ColumnData::Int { .. }
			| ColumnData::Uint { .. }
			| ColumnData::Decimal { .. }
	)
}

fn concat_strings(
	ctx: &EvaluationContext,
	l: &Utf8Container,
	r: &Utf8Container,
	target: Type,
	fragment: Fragment<'_>,
) -> crate::Result<Column> {
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l_str), Some(r_str)) => {
				let concatenated =
					format!("{}{}", l_str, r_str);
				data.push(concatenated);
			}
			_ => data.push_undefined(),
		}
	}
	Ok(Column::ColumnQualified(ColumnQualified {
		name: fragment.text().into(),
		data,
	}))
}

fn concat_string_with_other(
	ctx: &EvaluationContext,
	string_data: &Utf8Container,
	other_data: &ColumnData,
	string_is_left: bool,
	target: Type,
	fragment: Fragment<'_>,
) -> crate::Result<Column> {
	debug_assert_eq!(string_data.len(), other_data.len());

	let mut data = ctx.pooled(target, string_data.len());
	for i in 0..string_data.len() {
		match (string_data.get(i), other_data.is_defined(i)) {
			(Some(str_val), true) => {
				let other_str = other_data.as_string(i);
				let concatenated = if string_is_left {
					format!("{}{}", str_val, other_str)
				} else {
					format!("{}{}", other_str, str_val)
				};
				data.push(concatenated);
			}
			_ => data.push_undefined(),
		}
	}
	Ok(Column::ColumnQualified(ColumnQualified {
		name: fragment.text().into(),
		data,
	}))
}
