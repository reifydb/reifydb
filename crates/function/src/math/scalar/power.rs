// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct Power;

impl Power {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Power {
	fn scalar(&self, ctx: ScalarFunctionContext) -> reifydb_type::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() < 2 {
			return Ok(ColumnData::int4(Vec::<i32>::new()));
		}

		let base_column = columns.get(0).unwrap();
		let exponent_column = columns.get(1).unwrap();

		match (base_column.data(), exponent_column.data()) {
			(ColumnData::Int1(base_container), ColumnData::Int1(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					let power_result = match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								(base_val as i32).pow(exp_val as u32)
							}
						}
						_ => 0, // If either value is undefined, result is 0
					};

					result.push(power_result);
				}

				Ok(ColumnData::int4(result))
			}
			(ColumnData::Int2(base_container), ColumnData::Int2(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					let power_result = match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								(base_val as i32).pow(exp_val as u32)
							}
						}
						_ => 0, // If either value is undefined, result is 0
					};

					result.push(power_result);
				}

				Ok(ColumnData::int4(result))
			}
			(ColumnData::Int4(base_container), ColumnData::Int4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					let power_result = match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								base_val.saturating_pow(exp_val as u32)
							}
						}
						_ => 0, // If either value is undefined, result is 0
					};

					result.push(power_result);
				}

				Ok(ColumnData::int4(result))
			}
			(ColumnData::Int8(base_container), ColumnData::Int8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					let power_result = match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								base_val.saturating_pow(exp_val as u32)
							}
						}
						_ => 0, // If either value is undefined, result is 0
					};

					result.push(power_result);
				}

				Ok(ColumnData::int8(result))
			}
			_ => unimplemented!("Power function currently supports matching integer types only"),
		}
	}
}
