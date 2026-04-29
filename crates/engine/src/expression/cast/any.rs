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

	// First pass: validate all values can be cast to target type
	// We need to check all values before actually casting any
	let mut temp_results = Vec::with_capacity(any_container.len());

	for i in 0..any_container.len() {
		if !any_container.is_defined(i) {
			// Undefined values can be cast to any type
			temp_results.push(None);
			continue;
		}

		let value = &*any_container.data()[i];

		// Try to cast this single value to validate it can be done
		let single_column = ColumnBuffer::from(value.clone());
		match cast_column_data(ctx, &single_column, target.clone(), lazy_fragment.clone()) {
			Ok(result) => temp_results.push(Some(result)),
			Err(e) => {
				// If any value fails to cast, the entire operation fails
				return Err(e);
			}
		}
	}

	// Second pass: build the result container
	// All values validated successfully, now we can build the result
	let mut result = ColumnBuffer::with_capacity(target, any_container.len());

	for temp_result in temp_results {
		match temp_result {
			None => {
				// This was an undefined value
				result.push_none();
			}
			Some(casted_column) => {
				// Extract the single value from the casted column and add to result
				// We know each casted_column has exactly one value
				match &casted_column {
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
						// This shouldn't happen as we're casting FROM Any
						unreachable!("Casting from Any should not produce Any")
					}
					ColumnBuffer::Option {
						..
					} => {
						let value = casted_column.get_value(0);
						result.push_value(value);
					}
				}
			}
		}
	}

	Ok(result)
}
