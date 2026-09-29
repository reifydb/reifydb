// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		blob::Blob,
		column_view::{ColumnView, ViewData},
		container::{
			any_array,
			decimal_array::decimal_at,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			wide_int_array::wide_at,
		},
		value_type::ValueType,
	},
};

use super::{cast_column_data, convert::Convert};
use crate::value::column::{builder::ColumnBuilder, factory::from_many};

pub fn from_any(
	ctx: impl Convert + Copy,
	data: &ColumnView,
	target: ValueType,
	lazy_fragment: impl LazyFragment + Clone,
) -> Result<(FieldRef, ArrayRef)> {
	let name = data.field.name();
	let any_container = match &data.data {
		ViewData::Any {
			container,
			..
		} => *container,
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
		return Ok(ColumnBuilder::with_capacity(target.clone(), 0).finish(name));
	}

	let mut temp_results = Vec::with_capacity(any_container.len());

	for i in 0..any_container.len() {
		let Some(row) = any_array::get(any_container, i) else {
			temp_results.push(None);
			continue;
		};

		let value = row.unwrap_any();

		let single_column = from_many(name, value.clone(), 1);
		let single_view = ColumnView::try_from(&single_column)?;
		if let ViewData::Any {
			..
		} = single_view.data
		{
			return Err(TypeError::UnsupportedCast {
				from: data.get_type(),
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into());
		}
		match cast_column_data(ctx, &single_view, target.clone(), lazy_fragment.clone()) {
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
			Some(casted_column) if casted_column.0.is_nullable() => {
				result.push_value(ColumnView::try_from(&casted_column)?.get_value(0));
			}
			Some(casted_column) => match &ColumnView::try_from(&casted_column)?.data {
				ViewData::Bool(c) => {
					if !c.is_empty() {
						result.push::<bool>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Int1(c) => {
					if !c.is_empty() {
						result.push::<i8>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Int2(c) => {
					if !c.is_empty() {
						result.push::<i16>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Int4(c) => {
					if !c.is_empty() {
						result.push::<i32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Int8(c) => {
					if !c.is_empty() {
						result.push::<i64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Int16(c) => match wide_at::<i128>(c, 0) {
					Some(value) => result.push::<i128>(value),
					None => result.push_none(),
				},
				ViewData::Uint1(c) => {
					if !c.is_empty() {
						result.push::<u8>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Uint2(c) => {
					if !c.is_empty() {
						result.push::<u16>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Uint4(c) => {
					if !c.is_empty() {
						result.push::<u32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Uint8(c) => {
					if !c.is_empty() {
						result.push::<u64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Uint16(c) => match wide_at::<u128>(c, 0) {
					Some(value) => result.push::<u128>(value),
					None => result.push_none(),
				},
				ViewData::Float4(c) => {
					if !c.is_empty() {
						result.push::<f32>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Float8(c) => {
					if !c.is_empty() {
						result.push::<f64>(c.value(0));
					} else {
						result.push_none();
					}
				}
				ViewData::Utf8 {
					container: c,
					..
				} => {
					if !c.is_empty() {
						result.push::<String>(c.value(0).to_string());
					} else {
						result.push_none();
					}
				}
				ViewData::Blob {
					container: c,
					..
				} => {
					if !c.is_empty() {
						result.push(Blob::new(c.value(0).to_vec()));
					} else {
						result.push_none();
					}
				}
				ViewData::Date(c) => {
					if !c.is_empty() {
						result.push(dates(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::DateTime(c) => {
					if !c.is_empty() {
						result.push(datetimes(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::Time(c) => {
					if !c.is_empty() {
						result.push(times(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::Duration(c) => {
					if !c.is_empty() {
						result.push(durations(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::IdentityId(c) => {
					if !c.is_empty() {
						result.push(identity_ids(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::Uuid4(c) => {
					if !c.is_empty() {
						result.push(uuid4s(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::Uuid7(c) => {
					if !c.is_empty() {
						result.push(uuid7s(c)[0]);
					} else {
						result.push_none();
					}
				}
				ViewData::Decimal(c) => match decimal_at(c, 0) {
					Some(value) => result.push(value),
					None => result.push_none(),
				},
				ViewData::DictionaryId {
					container,
					..
				} => match dictionary_array::get(container, 0) {
					Some(entry) => result.push(entry),
					None => result.push_none(),
				},
				ViewData::Any {
					..
				} => {
					unreachable!("Casting from Any should not produce Any")
				}
				ViewData::Digest {
					..
				} => {
					let value = ColumnView::try_from(&casted_column)?.get_value(0);
					result.push_value(value);
				}
				ViewData::None {
					..
				} => {
					unreachable!("a none column is always nullable, so it takes the nullable arm")
				}
			},
		}
	}

	Ok(result.finish(name))
}
