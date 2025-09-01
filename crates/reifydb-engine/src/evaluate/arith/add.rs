// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_core::{
	Fragment, GetType, Type,
	interface::{Evaluator, evaluate::expression::AddExpression},
	return_error,
	value::{
		IsNumber,
		container::{
			NumberContainer, StringContainer, UndefinedContainer,
		},
    },
};
use reifydb_type::::diagnostic::operator::add_cannot_be_applied_to_incompatible_types;
use reifydb_type::{Promote, SafeAdd};
use crate::{
	columnar::{Column, ColumnData, ColumnQualified, push::Push},
	evaluate::{EvaluationContext, StandardEvaluator},
};

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
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float4(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int1(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint1(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// Float8
			(ColumnData::Float8(l), ColumnData::Float4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Float8(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int1(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint1(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Float8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// Signed × Signed
			(ColumnData::Int1(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int2(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int4(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int8(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int16(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// Signed × Unsigned
			(ColumnData::Int1(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int1(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int2(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int2(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int4(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int4(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int8(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int8(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Int16(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Int16(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// Unsigned × Signed
			(ColumnData::Uint1(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint2(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint4(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint8(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint16(l), ColumnData::Int1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Int2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Int4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Int8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Int16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// Unsigned × Unsigned
			(ColumnData::Uint1(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint1(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint2(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint2(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint4(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint4(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint8(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint8(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			(ColumnData::Uint16(l), ColumnData::Uint1(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Uint2(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Uint4(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Uint8(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}
			(ColumnData::Uint16(l), ColumnData::Uint16(r)) => {
				add_numeric(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// String concatenation
			(ColumnData::Utf8(l), ColumnData::Utf8(r)) => {
				concat_strings(
					ctx,
					l,
					r,
					target,
					add.full_fragment_owned(),
				)
			}

			// String + Other types (auto-promote to string)
			(ColumnData::Utf8(l), r)
				if can_promote_to_string(r) =>
			{
				concat_string_with_other(
					ctx,
					l,
					r,
					true,
					target,
					add.full_fragment_owned(),
				)
			}

			// Other types + String (auto-promote to string)
			(l, ColumnData::Utf8(r))
				if can_promote_to_string(l) =>
			{
				concat_string_with_other(
					ctx,
					r,
					l,
					false,
					target,
					add.full_fragment_owned(),
				)
			}

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
					add.full_fragment_owned(),
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
	fragment: Fragment<'_>,
) -> crate::Result<Column>
where
	L: GetType + Promote<R> + Copy + IsNumber + Clone + Debug + Default,
	R: GetType + IsNumber + Copy + Clone + Debug + Default,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeAdd,
	ColumnData: Push<<L as Promote<R>>::Output>,
{
	debug_assert_eq!(l.len(), r.len());

	let owned_fragment = fragment.into_owned();
	let mut data = ctx.pooled(target, l.len());
	for i in 0..l.len() {
		match (l.get(i), r.get(i)) {
			(Some(l), Some(r)) => {
				if let Some(value) =
					ctx.add(*l, *r, &owned_fragment)?
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
		name: owned_fragment.text().into(),
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
			| ColumnData::Blob(_)
	)
}

fn concat_strings(
	ctx: &EvaluationContext,
	l: &StringContainer,
	r: &StringContainer,
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
	string_data: &StringContainer,
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
