// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::expression::ParameterExpression,
	value::columnar::{Column, ColumnComputed, ColumnData},
};
use reifydb_type::{Fragment, Value, diagnostic::engine, error};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn parameter<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		expr: &ParameterExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let value = match expr {
			ParameterExpression::Positional {
				fragment,
			} => {
				let index = fragment.text()[1..]
					.parse::<usize>()
					.map_err(|_| error!(engine::invalid_parameter_reference(fragment.clone())))?;

				ctx.params
					.get_positional(index - 1)
					.ok_or_else(|| error!(engine::parameter_not_found(fragment.clone())))?
			}
			ParameterExpression::Named {
				fragment,
			} => {
				let name = &fragment.text()[1..];

				ctx.params
					.get_named(name)
					.ok_or_else(|| error!(engine::parameter_not_found(fragment.clone())))?
			}
		};

		let column_data = match value {
			Value::Boolean(b) => ColumnData::bool(vec![*b; ctx.row_count]),
			Value::Float4(f) => ColumnData::float4(vec![f.value(); ctx.row_count]),
			Value::Float8(f) => ColumnData::float8(vec![f.value(); ctx.row_count]),
			Value::Int1(i) => ColumnData::int1(vec![*i; ctx.row_count]),
			Value::Int2(i) => ColumnData::int2(vec![*i; ctx.row_count]),
			Value::Int4(i) => ColumnData::int4(vec![*i; ctx.row_count]),
			Value::Int8(i) => ColumnData::int8(vec![*i; ctx.row_count]),
			Value::Int16(i) => ColumnData::int16(vec![*i; ctx.row_count]),
			Value::Uint1(u) => ColumnData::uint1(vec![*u; ctx.row_count]),
			Value::Uint2(u) => ColumnData::uint2(vec![*u; ctx.row_count]),
			Value::Uint4(u) => ColumnData::uint4(vec![*u; ctx.row_count]),
			Value::Uint8(u) => ColumnData::uint8(vec![*u; ctx.row_count]),
			Value::Uint16(u) => ColumnData::uint16(vec![*u; ctx.row_count]),
			Value::Utf8(s) => ColumnData::utf8(vec![s.clone(); ctx.row_count]),
			Value::Date(d) => ColumnData::date(vec![*d; ctx.row_count]),
			Value::DateTime(dt) => ColumnData::datetime(vec![*dt; ctx.row_count]),
			Value::Time(t) => ColumnData::time(vec![*t; ctx.row_count]),
			Value::Interval(i) => ColumnData::interval(vec![*i; ctx.row_count]),
			Value::Uuid4(u) => ColumnData::uuid4(vec![*u; ctx.row_count]),
			Value::Uuid7(u) => ColumnData::uuid7(vec![*u; ctx.row_count]),
			Value::Blob(b) => ColumnData::blob(vec![b.clone(); ctx.row_count]),
			Value::RowNumber(id) => ColumnData::row_number(vec![*id; ctx.row_count]),
			Value::IdentityId(id) => ColumnData::identity_id(vec![*id; ctx.row_count]),
			Value::Int(bi) => ColumnData::int(vec![bi.clone(); ctx.row_count]),
			Value::Uint(bu) => ColumnData::uint(vec![bu.clone(); ctx.row_count]),
			Value::Decimal(bd) => ColumnData::decimal(vec![bd.clone(); ctx.row_count]),
			Value::Undefined => ColumnData::undefined(ctx.row_count),
		};
		Ok(Column::Computed(ColumnComputed {
			name: Fragment::owned_internal("parameter"),
			data: column_data,
		}))
	}
}
