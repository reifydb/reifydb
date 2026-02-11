// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::engine,
	value::column::{Column, data::ColumnData},
};
use reifydb_rql::expression::ParameterExpression;
use reifydb_type::{error, fragment::Fragment, value::Value};

use crate::expression::context::EvalContext;

pub(crate) fn parameter_lookup(ctx: &EvalContext, expr: &ParameterExpression) -> crate::Result<Column> {
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
		Value::Duration(i) => ColumnData::duration(vec![*i; ctx.row_count]),
		Value::Uuid4(u) => ColumnData::uuid4(vec![*u; ctx.row_count]),
		Value::Uuid7(u) => ColumnData::uuid7(vec![*u; ctx.row_count]),
		Value::Blob(b) => ColumnData::blob(vec![b.clone(); ctx.row_count]),
		Value::IdentityId(id) => ColumnData::identity_id(vec![*id; ctx.row_count]),
		Value::DictionaryId(v) => ColumnData::dictionary_id(vec![v.clone(); ctx.row_count]),
		Value::Int(bi) => ColumnData::int(vec![bi.clone(); ctx.row_count]),
		Value::Uint(bu) => ColumnData::uint(vec![bu.clone(); ctx.row_count]),
		Value::Decimal(bd) => ColumnData::decimal(vec![bd.clone(); ctx.row_count]),
		Value::Undefined => ColumnData::undefined(ctx.row_count),
		Value::Type(_) | Value::Any(_) => unreachable!("Any/Type not supported as parameter"),
	};
	Ok(Column {
		name: Fragment::internal("parameter"),
		data: column_data,
	})
}
