// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_core::{
	GetType, OwnedSpan, Type,
	result::error::diagnostic::operator::rem_cannot_be_applied_to_incompatible_types,
	return_error,
	value::{
		IsNumber,
		container::NumberContainer,
		number::{Promote, SafeRemainder},
	},
};
use reifydb_rql::expression::RemExpression;

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, push::Push},
	evaluate::{EvaluationContext, Evaluator},
};

impl Evaluator {
	pub(crate) fn rem(
		&mut self,
		rem: &RemExpression,
		ctx: &EvaluationContext,
	) -> crate::Result<Column> {
		let left = self.evaluate(&rem.left, ctx)?;
		let right = self.evaluate(&rem.right, ctx)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, rem.span())
			}

			_ => return_error!(
				rem_cannot_be_applied_to_incompatible_types(
					rem.span(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn rem_numeric<L, R>(
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
	<L as Promote<R>>::Output: SafeRemainder,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) =
					ctx.remainder(*l, *r, &span)?
				{
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
