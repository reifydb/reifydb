// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_core::{
	GetType, OwnedSpan, Type,
	result::error::diagnostic::operator::add_cannot_be_applied_to_incompatible_types,
	return_error,
	value::{
		IsNumber,
		container::NumberContainer,
		number::{Promote, SafeAdd},
	},
};
use reifydb_rql::expression::AddExpression;

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, push::Push},
	evaluate::{EvaluationContext, Evaluator},
};

impl Evaluator {
	pub(crate) fn add(
		&mut self,
		add: &AddExpression,
		ctx: &EvaluationContext,
	) -> crate::Result<Column> {
		let left = self.evaluate(&add.left, ctx)?;
		let right = self.evaluate(&add.right, ctx)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				add_numeric(ctx, l, r, target, add.span())
			}

			_ => return_error!(
				add_cannot_be_applied_to_incompatible_types(
					add.span(),
					left.get_type(),
					right.get_type(),
				)
			),
		}
	}
}

fn add_numeric<L, R>(
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
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) = ctx.add(*l, *r, &span)? {
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
