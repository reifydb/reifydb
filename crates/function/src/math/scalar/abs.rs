// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct Abs;

impl Abs {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Abs {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Int1(container) => {
				if container.is_fully_defined() {
					// Fast path: all values are defined
					let data: Vec<i8> = container
						.data()
						.iter()
						.take(row_count)
						.map(|&value| {
							if value < 0 {
								value * -1
							} else {
								value
							}
						})
						.collect();
					Ok(ColumnData::int1(data))
				} else {
					// Slow path: some values may be
					// undefined
					let mut data = Vec::with_capacity(container.len());

					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							data.push(if *value < 0 {
								*value * -1
							} else {
								*value
							});
						} else {
							// Push default value
							// for undefined entries
							data.push(0);
						}
					}

					Ok(ColumnData::int1_with_bitvec(data, container.bitvec().clone()))
				}
			}
			ColumnData::Int2(container) => {
				if container.is_fully_defined() {
					// Fast path: all values are defined
					let data: Vec<i16> = container
						.data()
						.iter()
						.take(row_count)
						.map(|&value| {
							if value < 0 {
								value * -1
							} else {
								value
							}
						})
						.collect();
					Ok(ColumnData::int2(data))
				} else {
					// Slow path: some values may be
					// undefined
					let mut data = Vec::with_capacity(container.len());

					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							data.push(if *value < 0 {
								*value * -1
							} else {
								*value
							});
						} else {
							// Push default value
							// for undefined entries
							data.push(0);
						}
					}

					Ok(ColumnData::int2_with_bitvec(data, container.bitvec().clone()))
				}
			}
			ColumnData::Int4(container) => {
				if container.is_fully_defined() {
					// Fast path: all values are defined
					let data: Vec<i32> = container
						.data()
						.iter()
						.take(row_count)
						.map(|&value| {
							if value < 0 {
								value * -1
							} else {
								value
							}
						})
						.collect();
					Ok(ColumnData::int4(data))
				} else {
					// Slow path: some values may be undefined
					let mut data = Vec::with_capacity(container.len());

					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							data.push(if *value < 0 {
								*value * -1
							} else {
								*value
							});
						} else {
							// Push default value for undefined entries
							data.push(0);
						}
					}

					Ok(ColumnData::int4_with_bitvec(data, container.bitvec().clone()))
				}
			}
			ColumnData::Float4(container) => {
				if container.is_fully_defined() {
					// Fast path: all values are defined
					let data: Vec<f32> = container
						.data()
						.iter()
						.take(row_count)
						.map(|&value| value.abs())
						.collect();
					Ok(ColumnData::float4(data))
				} else {
					// Slow path: some values may be undefined
					let mut data = Vec::with_capacity(container.len());

					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							data.push(value.abs());
						} else {
							// Push default value for undefined entries
							data.push(0.0);
						}
					}

					Ok(ColumnData::float4_with_bitvec(data, container.bitvec().clone()))
				}
			}
			ColumnData::Float8(container) => {
				if container.is_fully_defined() {
					// Fast path: all values are defined
					let data: Vec<f64> = container
						.data()
						.iter()
						.take(row_count)
						.map(|&value| value.abs())
						.collect();
					Ok(ColumnData::float8(data))
				} else {
					// Slow path: some values may be undefined
					let mut data = Vec::with_capacity(container.len());

					for i in 0..row_count {
						if let Some(value) = container.get(i) {
							data.push(value.abs());
						} else {
							// Push default value for undefined entries
							data.push(0.0);
						}
					}

					Ok(ColumnData::float8_with_bitvec(data, container.bitvec().clone()))
				}
			}
			_ => unimplemented!(),
		}
	}
}
