// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ColumnEvaluator, evaluate::expression::RemExpression},
	value::{
		column::{Column, ColumnData, push::Push},
		container::{NumberContainer, UndefinedContainer},
	},
};
use reifydb_type::{
	GetType, IsNumber, LazyFragment, Promote, SafeRemainder, Type,
	diagnostic::operator::rem_cannot_be_applied_to_incompatible_types, return_error,
};

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn rem<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		rem: &RemExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let left = self.evaluate(ctx, &rem.left)?;
		let right = self.evaluate(ctx, &rem.right)?;
		let target = Type::promote(left.get_type(), right.get_type());

		match (&left.data(), &right.data()) {
			// Float4
			(ColumnData::Float4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				rem_numeric(ctx, l, r, target, || rem.full_fragment_owned())
			}

			// Int with other types
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			// Uint with other types
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			// Decimal with other types
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Int16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint1(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint2(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Uint16(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float4(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Decimal {
					container: l,
					..
				},
				ColumnData::Float8(r),
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			// Standard types with Int, Uint, Decimal
			(
				ColumnData::Int1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Int16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			(
				ColumnData::Uint1(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint2(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint16(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint1(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint2(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint16(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint1(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint2(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Uint16(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			(
				ColumnData::Float4(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Float4(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Float4(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Float8(l),
				ColumnData::Int {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Float8(l),
				ColumnData::Uint {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),
			(
				ColumnData::Float8(l),
				ColumnData::Decimal {
					container: r,
					..
				},
			) => rem_numeric_clone(ctx, l, r, target, || rem.full_fragment_owned()),

			// Handle undefined values - any operation with
			// undefined results in undefined
			(ColumnData::Undefined(l), _) => Ok(Column {
				name: rem.full_fragment_owned(),
				data: ColumnData::Undefined(UndefinedContainer::new(l.len())),
			}),
			(_, ColumnData::Undefined(r)) => Ok(Column {
				name: rem.full_fragment_owned(),
				data: ColumnData::Undefined(UndefinedContainer::new(r.len())),
			}),

			_ => return_error!(rem_cannot_be_applied_to_incompatible_types(
				rem.full_fragment_owned(),
				left.get_type(),
				right.get_type(),
			)),
		}
	}
}

fn rem_numeric<'a, L, R>(
	ctx: &ColumnEvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment<'a> + Copy,
) -> crate::Result<Column<'a>>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeRemainder,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			if let Some(value) = ctx.remainder(&l_data[i], &r_data[i], fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l), Some(r)) => {
					if let Some(value) = ctx.remainder(l, r, fragment)? {
						data.push(value);
					} else {
						data.push_undefined()
					}
				}
				_ => data.push_undefined(),
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	}
}

fn rem_numeric_clone<'a, L, R>(
	ctx: &ColumnEvaluationContext,
	l: &NumberContainer<L>,
	r: &NumberContainer<R>,
	target: Type,
	fragment: impl LazyFragment<'a> + Copy,
) -> crate::Result<Column<'a>>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeRemainder,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	if l.is_fully_defined() && r.is_fully_defined() {
		// Fast path: all values are defined, no undefined checks needed
		let mut data = ctx.pooled(target, l.len());
		let l_data = l.data();
		let r_data = r.data();

		for i in 0..l.len() {
			let l_clone = l_data[i].clone();
			let r_clone = r_data[i].clone();
			if let Some(value) = ctx.remainder(&l_clone, &r_clone, fragment)? {
				data.push(value);
			} else {
				data.push_undefined()
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	} else {
		// Slow path: some values may be undefined
		let mut data = ctx.pooled(target, l.len());
		for i in 0..l.len() {
			match (l.get(i), r.get(i)) {
				(Some(l_val), Some(r_val)) => {
					let l_clone = l_val.clone();
					let r_clone = r_val.clone();
					if let Some(value) = ctx.remainder(&l_clone, &r_clone, fragment)? {
						data.push(value);
					} else {
						data.push_undefined()
					}
				}
				_ => data.push_undefined(),
			}
		}

		Ok(Column {
			name: fragment.fragment(),
			data,
		})
	}
}
