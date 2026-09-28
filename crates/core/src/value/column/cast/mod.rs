// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod any;
pub mod blob;
pub mod boolean;
pub mod convert;
pub mod error;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use arrow_buffer::NullBuffer;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	value::{Value, value_type::ValueType},
};

use self::{
	convert::{Convert, TargetConvert},
	uuid::to_uuid,
};
use crate::value::column::buffer::ColumnBuffer;

pub fn cast_value(value: Value, target: &ValueType) -> Result<Value> {
	if value.get_type() == *target {
		return Ok(value);
	}
	let data = ColumnBuffer::from(value.clone());
	let display = value.to_string();
	let cast = cast_column_data(
		TargetConvert {
			target: None,
		},
		&data,
		target.clone(),
		|| Fragment::internal(display.clone()),
	)?;
	Ok(cast.get_value(0))
}

pub fn cast_column_data(
	ctx: impl Convert + Copy,
	data: &ColumnBuffer,
	target: ValueType,
	lazy_fragment: impl LazyFragment + Clone,
) -> Result<ColumnBuffer> {
	if let Some(nulls) = data.nulls()
		&& !data.keeps_own_nulls()
	{
		let (inner, _) = data.clone().split_nulls();
		let bitvec = nulls.inner();
		let inner_target = match &target {
			ValueType::Option(t) => t.as_ref().clone(),
			other => other.clone(),
		};
		let total_len = inner.len();
		let defined_count = bitvec.count_set_bits();

		if defined_count == 0 {
			return Ok(ColumnBuffer::none_typed(inner_target, total_len));
		}

		if defined_count < total_len {
			let mut compacted = inner;
			compacted.filter(bitvec)?;

			let cast_compacted = cast_column_data(ctx, &compacted, inner_target, lazy_fragment)?;

			let mut expand_picks = Vec::with_capacity(total_len);
			let mut src_idx = 0usize;
			for i in 0..total_len {
				if bitvec.value(i) {
					expand_picks.push(Some(src_idx));
					src_idx += 1;
				} else {
					expand_picks.push(None);
				}
			}
			return cast_compacted.extract_rows_or_none(&expand_picks);
		}

		let cast_inner = cast_column_data(ctx, &inner, inner_target, lazy_fragment)?;
		return Ok(match cast_inner.nulls() {
			Some(_) => cast_inner,
			None => cast_inner.with_nulls(nulls.clone()),
		});
	}

	if let ValueType::Option(inner_target) = &target {
		let cast_inner = cast_column_data(ctx, data, *inner_target.clone(), lazy_fragment)?;
		return Ok(match cast_inner.nulls() {
			Some(_) => cast_inner,
			None => {
				let len = cast_inner.len();
				cast_inner.with_nulls(NullBuffer::new_valid(len))
			}
		});
	}

	let object_type = match data.get_type() {
		ValueType::Option(inner) if data.keeps_own_nulls() => *inner,
		other => other,
	};
	if target == object_type {
		return Ok(data.clone());
	}
	match (&object_type, &target) {
		(ValueType::Any, _) => any::from_any(ctx, data, target, lazy_fragment),
		(_, t) if t.is_number() => number::to_number(ctx, data, target, lazy_fragment),
		(_, t) if t.is_blob() => blob::to_blob(data, lazy_fragment),
		(_, t) if t.is_bool() => boolean::to_boolean(data, lazy_fragment),
		(_, t) if t.is_utf8() => text::to_text(data, lazy_fragment),
		(_, t) if t.is_temporal() => temporal::to_temporal(data, target, lazy_fragment),
		(_, ValueType::IdentityId) => to_uuid(data, target, lazy_fragment),
		(ValueType::IdentityId, _) => to_uuid(data, target, lazy_fragment),
		(_, t) if t.is_uuid() => to_uuid(data, target, lazy_fragment),
		(source, t) if source.is_uuid() || t.is_uuid() => to_uuid(data, target, lazy_fragment),
		_ => Err(TypeError::UnsupportedCast {
			from: object_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}
