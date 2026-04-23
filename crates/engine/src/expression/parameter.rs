// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::engine,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_rql::expression::ParameterExpression;
use reifydb_type::{
	error,
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn parameter_lookup(ctx: &EvalContext, expr: &ParameterExpression) -> Result<ColumnWithName> {
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
		Value::Boolean(b) => ColumnBuffer::bool(vec![*b; ctx.row_count]),
		Value::Float4(f) => ColumnBuffer::float4(vec![f.value(); ctx.row_count]),
		Value::Float8(f) => ColumnBuffer::float8(vec![f.value(); ctx.row_count]),
		Value::Int1(i) => ColumnBuffer::int1(vec![*i; ctx.row_count]),
		Value::Int2(i) => ColumnBuffer::int2(vec![*i; ctx.row_count]),
		Value::Int4(i) => ColumnBuffer::int4(vec![*i; ctx.row_count]),
		Value::Int8(i) => ColumnBuffer::int8(vec![*i; ctx.row_count]),
		Value::Int16(i) => ColumnBuffer::int16(vec![*i; ctx.row_count]),
		Value::Uint1(u) => ColumnBuffer::uint1(vec![*u; ctx.row_count]),
		Value::Uint2(u) => ColumnBuffer::uint2(vec![*u; ctx.row_count]),
		Value::Uint4(u) => ColumnBuffer::uint4(vec![*u; ctx.row_count]),
		Value::Uint8(u) => ColumnBuffer::uint8(vec![*u; ctx.row_count]),
		Value::Uint16(u) => ColumnBuffer::uint16(vec![*u; ctx.row_count]),
		Value::Utf8(s) => ColumnBuffer::utf8(vec![s.clone(); ctx.row_count]),
		Value::Date(d) => ColumnBuffer::date(vec![*d; ctx.row_count]),
		Value::DateTime(dt) => ColumnBuffer::datetime(vec![*dt; ctx.row_count]),
		Value::Time(t) => ColumnBuffer::time(vec![*t; ctx.row_count]),
		Value::Duration(i) => ColumnBuffer::duration(vec![*i; ctx.row_count]),
		Value::Uuid4(u) => ColumnBuffer::uuid4(vec![*u; ctx.row_count]),
		Value::Uuid7(u) => ColumnBuffer::uuid7(vec![*u; ctx.row_count]),
		Value::Blob(b) => ColumnBuffer::blob(vec![b.clone(); ctx.row_count]),
		Value::IdentityId(id) => ColumnBuffer::identity_id(vec![*id; ctx.row_count]),
		Value::DictionaryId(v) => ColumnBuffer::dictionary_id(vec![*v; ctx.row_count]),
		Value::Int(bi) => ColumnBuffer::int(vec![bi.clone(); ctx.row_count]),
		Value::Uint(bu) => ColumnBuffer::uint(vec![bu.clone(); ctx.row_count]),
		Value::Decimal(bd) => ColumnBuffer::decimal(vec![bd.clone(); ctx.row_count]),
		Value::None {
			..
		} => ColumnBuffer::none_typed(Type::Boolean, ctx.row_count),
		Value::Type(_) | Value::Any(_) | Value::List(_) | Value::Record(_) | Value::Tuple(_) => {
			unreachable!("Any/Type/List/Record/Tuple not supported as parameter")
		}
	};
	Ok(ColumnWithName::new(Fragment::internal("parameter"), column_data))
}
