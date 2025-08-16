// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_core::{
	GetType, OwnedSpan, Type,
	interface::{Evaluator, evaluate::expression::SubExpression},
	result::error::diagnostic::operator::sub_cannot_be_applied_to_incompatible_types,
	return_error,
	value::{
		IsNumber,
		container::number::NumberContainer,
		number::{Promote, SafeSub},
	},
};

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, push::Push},
	evaluate::{EvaluationContext, StandardEvaluator},
};

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
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				sub_numeric(ctx, l, r, target, sub.span())
			}

			_ => return_error!(
				sub_cannot_be_applied_to_incompatible_types(
					sub.span(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn sub_numeric<L, R>(
	ctx: &EvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	span: OwnedSpan,
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
				if let Some(value) = ctx.sub(*l, *r, &span)? {
					data.push(value);
				} else {
					data.push_undefined()
				}
			}
			_ => data.push_undefined(),
		}
	}

	Ok(Column::ColumnQualified(ColumnQualified {
		name: span.fragment.into(),
		data,
	}))
}
