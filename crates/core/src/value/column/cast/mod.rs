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
pub mod vector;

use reifydb_value::{
	Result,
	error::TypeError,
	fragment::{Fragment, LazyFragment},
	storage::DataBitVec,
	util::bitvec::BitVec,
	value::{Value, constraint::TypeConstraint, value_type::ValueType},
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
	if let ColumnBuffer::Option {
		inner,
		bitvec,
	} = data
	{
		let inner_target = match &target {
			ValueType::Option(t) => t.as_ref().clone(),
			other => other.clone(),
		};
		let total_len = inner.len();
		let defined_count = DataBitVec::count_ones(bitvec);

		if defined_count == 0 {
			return Ok(ColumnBuffer::none_typed(inner_target, total_len));
		}

		if defined_count < total_len {
			let mut compacted = inner.as_ref().clone();
			compacted.filter(bitvec)?;

			let mut cast_compacted = cast_column_data(ctx, &compacted, inner_target, lazy_fragment)?;

			let sentinel = defined_count;
			let mut expand_indices = Vec::with_capacity(total_len);
			let mut src_idx = 0usize;
			for i in 0..total_len {
				if DataBitVec::get(bitvec, i) {
					expand_indices.push(src_idx);
					src_idx += 1;
				} else {
					expand_indices.push(sentinel);
				}
			}
			cast_compacted.reorder(&expand_indices);

			return Ok(match cast_compacted {
				already @ ColumnBuffer::Option {
					..
				} => already,
				other => ColumnBuffer::Option {
					inner: Box::new(other),
					bitvec: bitvec.clone(),
				},
			});
		}

		let cast_inner = cast_column_data(ctx, inner, inner_target, lazy_fragment)?;
		return Ok(match cast_inner {
			already @ ColumnBuffer::Option {
				..
			} => already,
			other => ColumnBuffer::Option {
				inner: Box::new(other),
				bitvec: bitvec.clone(),
			},
		});
	}

	if let ValueType::Option(inner_target) = &target {
		let cast_inner = cast_column_data(ctx, data, *inner_target.clone(), lazy_fragment)?;
		return Ok(match cast_inner {
			already @ ColumnBuffer::Option {
				..
			} => already,
			other => {
				let bitvec = BitVec::repeat(other.len(), true);
				ColumnBuffer::Option {
					inner: Box::new(other),
					bitvec,
				}
			}
		});
	}

	let shape_type = data.get_type();
	if target == shape_type {
		return Ok(data.clone());
	}
	match (&shape_type, &target) {
		(_, ValueType::Vector(dims)) => vector::to_vector(data, *dims, lazy_fragment),
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
			from: shape_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}

pub fn cast_column_data_constrained(
	ctx: impl Convert + Copy,
	data: &ColumnBuffer,
	target: &TypeConstraint,
	lazy_fragment: impl LazyFragment + Clone,
) -> Result<ColumnBuffer> {
	let cast = cast_column_data(ctx, data, target.get_type(), lazy_fragment)?;
	if target.constraint().is_none() {
		return Ok(cast);
	}
	for idx in 0..cast.len() {
		if cast.is_defined(idx) {
			target.validate(&cast.get_value(idx))?;
		}
	}
	Ok(cast)
}
