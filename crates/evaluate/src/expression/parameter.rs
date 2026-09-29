// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{error::diagnostic::engine, expression::ParameterExpression, value::column::factory};
use reifydb_value::{error, value::Value};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn parameter_lookup(ctx: &EvalContext, expr: &ParameterExpression) -> Result<(FieldRef, ArrayRef)> {
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

	Ok(match value {
		Value::Boolean(b) => factory::bool("parameter", vec![*b; ctx.row_count]),
		Value::Float4(f) => factory::float4("parameter", vec![f.value(); ctx.row_count]),
		Value::Float8(f) => factory::float8("parameter", vec![f.value(); ctx.row_count]),
		Value::Int1(i) => factory::int1("parameter", vec![*i; ctx.row_count]),
		Value::Int2(i) => factory::int2("parameter", vec![*i; ctx.row_count]),
		Value::Int4(i) => factory::int4("parameter", vec![*i; ctx.row_count]),
		Value::Int8(i) => factory::int8("parameter", vec![*i; ctx.row_count]),
		Value::Int16(i) => factory::int16("parameter", vec![*i; ctx.row_count]),
		Value::Uint1(u) => factory::uint1("parameter", vec![*u; ctx.row_count]),
		Value::Uint2(u) => factory::uint2("parameter", vec![*u; ctx.row_count]),
		Value::Uint4(u) => factory::uint4("parameter", vec![*u; ctx.row_count]),
		Value::Uint8(u) => factory::uint8("parameter", vec![*u; ctx.row_count]),
		Value::Uint16(u) => factory::uint16("parameter", vec![*u; ctx.row_count]),
		Value::Utf8(s) => factory::utf8("parameter", vec![s.clone(); ctx.row_count]),
		Value::Date(d) => factory::date("parameter", vec![*d; ctx.row_count]),
		Value::DateTime(dt) => factory::datetime("parameter", vec![*dt; ctx.row_count]),
		Value::Time(t) => factory::time("parameter", vec![*t; ctx.row_count]),
		Value::Duration(i) => factory::duration("parameter", vec![*i; ctx.row_count]),
		Value::Uuid4(u) => factory::uuid4("parameter", vec![*u; ctx.row_count]),
		Value::Uuid7(u) => factory::uuid7("parameter", vec![*u; ctx.row_count]),
		Value::Blob(b) => factory::blob("parameter", vec![b.clone(); ctx.row_count]),
		Value::IdentityId(id) => factory::identity_id("parameter", vec![*id; ctx.row_count]),
		Value::DictionaryId(v) => factory::dictionary_id("parameter", vec![*v; ctx.row_count]),
		Value::Decimal(_) => factory::from_many("parameter", value.clone(), ctx.row_count),
		Value::None {
			..
		} => factory::from_many("parameter", value.clone(), ctx.row_count),
		Value::Digest(_) => factory::from_many("parameter", value.clone(), ctx.row_count),
		Value::Type(_) | Value::Any(_) | Value::List(_) | Value::Record(_) | Value::Tuple(_) => {
			unreachable!("Any/ValueType/List/Record/Tuple not supported as parameter")
		}
	})
}
