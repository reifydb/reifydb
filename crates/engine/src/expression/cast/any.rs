// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::{
	error::TypeError,
	fragment::LazyFragment,
	value::{blob::Blob, r#type::Type},
};

use super::cast_column_data;
use crate::{Result, expression::context::EvalContext};

pub fn from_any(
	ctx: &EvalContext,
	data: &ColumnBuffer,
	target: Type,
	lazy_fragment: impl LazyFragment + Clone,
) -> Result<ColumnBuffer> {
	let any_container = match data {
		ColumnBuffer::Any(container) => container,
		_ => {
			return Err(TypeError::UnsupportedCast {
				from: data.get_type(),
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into());
		}
	};

	if any_container.is_empty() {
		return Ok(ColumnBuffer::with_capacity(target.clone(), 0));
	}

	let mut temp_results = Vec::with_capacity(any_container.len());

	for i in 0..any_container.len() {
		if !any_container.is_defined(i) {
			temp_results.push(None);
			continue;
		}

		let value = &*any_container.data()[i];

		let single_column = ColumnBuffer::from(value.clone());
		match cast_column_data(ctx, &single_column, target.clone(), lazy_fragment.clone()) {
			Ok(result) => temp_results.push(Some(result)),
			Err(e) => {
				return Err(e);
			}
		}
	}

	let mut result = ColumnBuffer::with_capacity(target, any_container.len());

	for temp_result in temp_results {
		match temp_result {
			None => {
				result.push_none();
			}
			Some(casted_column) => match &casted_column {
				ColumnBuffer::Bool(c) => {
					if c.is_defined(0) {
						result.push::<bool>(c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int1(c) => {
					if c.is_defined(0) {
						result.push::<i8>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int2(c) => {
					if c.is_defined(0) {
						result.push::<i16>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int4(c) => {
					if c.is_defined(0) {
						result.push::<i32>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int8(c) => {
					if c.is_defined(0) {
						result.push::<i64>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int16(c) => {
					if c.is_defined(0) {
						result.push::<i128>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint1(c) => {
					if c.is_defined(0) {
						result.push::<u8>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint2(c) => {
					if c.is_defined(0) {
						result.push::<u16>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint4(c) => {
					if c.is_defined(0) {
						result.push::<u32>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint8(c) => {
					if c.is_defined(0) {
						result.push::<u64>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint16(c) => {
					if c.is_defined(0) {
						result.push::<u128>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Float4(c) => {
					if c.is_defined(0) {
						result.push::<f32>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Float8(c) => {
					if c.is_defined(0) {
						result.push::<f64>(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Utf8 {
					container: c,
					..
				} => {
					if c.is_defined(0) {
						result.push::<String>(c.get(0).unwrap().to_string());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Blob {
					container: c,
					..
				} => {
					if c.is_defined(0) {
						result.push(Blob::new(c.get(0).unwrap().to_vec()));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Date(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::DateTime(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Time(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Duration(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::IdentityId(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uuid4(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uuid7(c) => {
					if c.is_defined(0) {
						result.push(*c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int {
					container: c,
					..
				} => {
					if c.is_defined(0) {
						result.push(c.get(0).unwrap().clone());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint {
					container: c,
					..
				} => {
					if c.is_defined(0) {
						result.push(c.get(0).unwrap().clone());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Decimal {
					container: c,
					..
				} => {
					if c.is_defined(0) {
						result.push(c.get(0).unwrap().clone());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::DictionaryId(c) => {
					if c.is_defined(0) {
						result.push(c.get(0).unwrap());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Any(_) => {
					unreachable!("Casting from Any should not produce Any")
				}
				ColumnBuffer::Option {
					..
				} => {
					let value = casted_column.get_value(0);
					result.push_value(value);
				}
			},
		}
	}

	Ok(result)
}
