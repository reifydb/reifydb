// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Array;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		blob::Blob,
		container::{
			decimal_array::u128_at,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
		},
		value_type::ValueType,
	},
};

use super::{cast_column_data, convert::Convert};
use crate::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};

pub fn from_any(
	ctx: impl Convert + Copy,
	data: &ColumnBuffer,
	target: ValueType,
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
		return Ok(ColumnBuilder::with_capacity(target.clone(), 0).finish());
	}

	let mut temp_results = Vec::with_capacity(any_container.len());

	for i in 0..any_container.len() {
		if !any_container.is_defined(i) {
			temp_results.push(None);
			continue;
		}

		let value = any_container.data()[i].unwrap_any();

		let single_column = ColumnBuffer::from(value.clone());
		if let ColumnBuffer::Any(_) = single_column {
			return Err(TypeError::UnsupportedCast {
				from: data.get_type(),
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into());
		}
		match cast_column_data(ctx, &single_column, target.clone(), lazy_fragment.clone()) {
			Ok(result) => temp_results.push(Some(result)),
			Err(e) => {
				return Err(e);
			}
		}
	}

	let mut result = ColumnBuilder::with_capacity(target, any_container.len());

	for temp_result in temp_results {
		match temp_result {
			None => {
				result.push_none();
			}
			Some(casted_column) => match &casted_column {
				ColumnBuffer::Bool(c) => {
					if !c.is_empty() {
						result.push::<bool>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int1(c) => {
					if !c.is_empty() {
						result.push::<i8>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int2(c) => {
					if !c.is_empty() {
						result.push::<i16>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int4(c) => {
					if !c.is_empty() {
						result.push::<i32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int8(c) => {
					if !c.is_empty() {
						result.push::<i64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Int16(c) => {
					if !c.is_empty() {
						result.push::<i128>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint1(c) => {
					if !c.is_empty() {
						result.push::<u8>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint2(c) => {
					if !c.is_empty() {
						result.push::<u16>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint4(c) => {
					if !c.is_empty() {
						result.push::<u32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint8(c) => {
					if !c.is_empty() {
						result.push::<u64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uint16(c) => match u128_at(c, 0) {
					Some(value) => result.push::<u128>(value),
					None => result.push_none(),
				},
				ColumnBuffer::Float4(c) => {
					if !c.is_empty() {
						result.push::<f32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Float8(c) => {
					if !c.is_empty() {
						result.push::<f64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Utf8 {
					container: c,
					..
				} => {
					if !c.is_empty() {
						result.push::<String>(c.value(0).to_string());
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Blob {
					container: c,
					..
				} => {
					if !c.is_empty() {
						result.push(Blob::new(c.value(0).to_vec()));
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Date(c) => {
					if !c.is_empty() {
						result.push(dates(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::DateTime(c) => {
					if !c.is_empty() {
						result.push(datetimes(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Time(c) => {
					if !c.is_empty() {
						result.push(times(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Duration(c) => {
					if !c.is_empty() {
						result.push(durations(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::IdentityId(c) => {
					if !c.is_empty() {
						result.push(*identity_ids(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uuid4(c) => {
					if !c.is_empty() {
						result.push(uuid4s(c)[0]);
					} else {
						result.push_none();
					}
				}
				ColumnBuffer::Uuid7(c) => {
					if !c.is_empty() {
						result.push(uuid7s(c)[0]);
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
				ColumnBuffer::DictionaryId {
					container,
					..
				} => match dictionary_array::get(container, 0) {
					Some(entry) => result.push(entry),
					None => result.push_none(),
				},
				ColumnBuffer::Any(_) => {
					unreachable!("Casting from Any should not produce Any")
				}
				ColumnBuffer::Option {
					..
				}
				| ColumnBuffer::Digest {
					..
				} => {
					let value = casted_column.get_value(0);
					result.push_value(value);
				}
			},
		}
	}

	Ok(result.finish())
}
