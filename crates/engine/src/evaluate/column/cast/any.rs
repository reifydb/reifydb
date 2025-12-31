// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::value::column::ColumnData;
use reifydb_type::{LazyFragment, Type, diagnostic::cast, err};

use super::cast_column_data;
use crate::evaluate::ColumnEvaluationContext;

pub fn from_any(
	ctx: &ColumnEvaluationContext,
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment + Clone,
) -> crate::Result<ColumnData> {
	let any_container = match data {
		ColumnData::Any(container) => container,
		_ => return err!(cast::unsupported_cast(lazy_fragment.fragment(), data.get_type(), target)),
	};

	if any_container.is_empty() {
		return Ok(ColumnData::with_capacity(target, 0));
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
		let single_column = ColumnData::from(value.clone());
		match cast_column_data(ctx, &single_column, target, lazy_fragment.clone()) {
			Ok(result) => temp_results.push(Some(result)),
			Err(e) => {
				// If any value fails to cast, the entire operation fails
				return Err(e);
			}
		}
	}

	// Second pass: build the result container
	// All values validated successfully, now we can build the result
	let mut result = ColumnData::with_capacity(target, any_container.len());

	for temp_result in temp_results {
		match temp_result {
			None => {
				// This was an undefined value
				result.push_undefined();
			}
			Some(casted_column) => {
				// Extract the single value from the casted column and add to result
				// We know each casted_column has exactly one value
				match &casted_column {
					ColumnData::Bool(c) => {
						if c.is_defined(0) {
							result.push::<bool>(c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int1(c) => {
						if c.is_defined(0) {
							result.push::<i8>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int2(c) => {
						if c.is_defined(0) {
							result.push::<i16>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int4(c) => {
						if c.is_defined(0) {
							result.push::<i32>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int8(c) => {
						if c.is_defined(0) {
							result.push::<i64>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int16(c) => {
						if c.is_defined(0) {
							result.push::<i128>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint1(c) => {
						if c.is_defined(0) {
							result.push::<u8>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint2(c) => {
						if c.is_defined(0) {
							result.push::<u16>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint4(c) => {
						if c.is_defined(0) {
							result.push::<u32>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint8(c) => {
						if c.is_defined(0) {
							result.push::<u64>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint16(c) => {
						if c.is_defined(0) {
							result.push::<u128>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Float4(c) => {
						if c.is_defined(0) {
							result.push::<f32>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Float8(c) => {
						if c.is_defined(0) {
							result.push::<f64>(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Utf8 {
						container: c,
						..
					} => {
						if c.is_defined(0) {
							result.push::<String>(c.get(0).unwrap().clone());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Blob {
						container: c,
						..
					} => {
						if c.is_defined(0) {
							result.push(c.get(0).unwrap().clone());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Date(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::DateTime(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Time(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Duration(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::IdentityId(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uuid4(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uuid7(c) => {
						if c.is_defined(0) {
							result.push(*c.get(0).unwrap());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Int {
						container: c,
						..
					} => {
						if c.is_defined(0) {
							result.push(c.get(0).unwrap().clone());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Uint {
						container: c,
						..
					} => {
						if c.is_defined(0) {
							result.push(c.get(0).unwrap().clone());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Decimal {
						container: c,
						..
					} => {
						if c.is_defined(0) {
							result.push(c.get(0).unwrap().clone());
						} else {
							result.push_undefined();
						}
					}
					ColumnData::Undefined(_) => {
						result.push_undefined();
					}
					ColumnData::Any(_) => {
						// This shouldn't happen as we're casting FROM Any
						unreachable!("Casting from Any should not produce Any")
					}
				}
			}
		}
	}

	Ok(result)
}
