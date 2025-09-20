// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::columnar::ColumnData;

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct Abs;

impl Abs {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Abs {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
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
			_ => unimplemented!(),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		BitVec,
		value::columnar::{ColumnQualified, Columns},
	};

	use super::*;

	#[test]
	fn test_abs_int1_fully_defined() {
		let function = Abs::new();

		// Create a column with all values defined
		let data = vec![-5i8, 3, -2, 0, 7, -1];
		let column = ColumnQualified::int1("test", data.clone());

		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 6,
		};

		let result = function.scalar(ctx).unwrap();

		// Check that result is fully defined
		if let ColumnData::Int1(container) = result {
			assert!(container.is_fully_defined());
			assert_eq!(container.len(), 6);

			// Check values
			let expected = vec![5i8, 3, 2, 0, 7, 1];
			for (i, &expected_val) in expected.iter().enumerate() {
				assert_eq!(container.get(i), Some(&expected_val));
			}
		} else {
			panic!("Expected Int1 result");
		}
	}

	#[test]
	fn test_abs_int1_partially_defined() {
		let function = Abs::new();

		// Create a column with some undefined values
		let data = vec![-5i8, 3, -2, 0, 7, -1];
		let mut bitvec = BitVec::repeat(6, true);
		bitvec.set(2, false); // Make index 2 undefined
		bitvec.set(4, false); // Make index 4 undefined

		let column = ColumnQualified::int1_with_bitvec("test", data.clone(), bitvec.clone());

		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 6,
		};

		let result = function.scalar(ctx).unwrap();

		// Check that result has same undefined pattern
		if let ColumnData::Int1(container) = result {
			assert!(!container.is_fully_defined());
			assert_eq!(container.bitvec().count_ones(), 4); // Only 4 defined values

			// Check defined values
			assert_eq!(container.get(0), Some(&5));
			assert_eq!(container.get(1), Some(&3));
			assert_eq!(container.get(2), None); // undefined
			assert_eq!(container.get(3), Some(&0));
			assert_eq!(container.get(4), None); // undefined
			assert_eq!(container.get(5), Some(&1));
		} else {
			panic!("Expected Int1 result");
		}
	}

	#[test]
	fn test_abs_int2_fully_defined() {
		let function = Abs::new();

		// Create a column with all values defined
		let data = vec![-500i16, 300, -200, 0, 700, -100];
		let column = ColumnQualified::int2("test", data.clone());

		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 6,
		};

		let result = function.scalar(ctx).unwrap();

		// Check that result is fully defined
		if let ColumnData::Int2(container) = result {
			assert!(container.is_fully_defined());
			assert_eq!(container.len(), 6);

			// Check values
			let expected = vec![500i16, 300, 200, 0, 700, 100];
			for (i, &expected_val) in expected.iter().enumerate() {
				assert_eq!(container.get(i), Some(&expected_val));
			}
		} else {
			panic!("Expected Int2 result");
		}
	}
}
