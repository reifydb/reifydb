// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::Result;

use crate::value::column::columns::Columns;

impl Columns {
	pub fn take(&mut self, n: usize) -> Result<()> {
		self.system.take(n);

		let mut new_buffers = Vec::with_capacity(self.len());

		for data in self.columns.iter() {
			new_buffers.push(data.take(n));
		}

		self.columns = new_buffers;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_buffer::NullBuffer;
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	use crate::value::column::{ColumnBuffer, ColumnWithName};

	#[test]
	fn test_bool_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"flag",
			ColumnBuffer::bool_with_bitvec([true, true, false], vec![false, true, true]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::bool_with_bitvec([true], vec![false]));
	}

	#[test]
	fn test_float4_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::float4_with_bitvec([1.0, 2.0, 3.0], vec![true, false, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::float4_with_bitvec([1.0, 2.0], vec![true, false]));
	}

	#[test]
	fn test_float8_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::float8_with_bitvec([1f64, 2.0, 3.0, 4.0], vec![true, true, false, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::float8([1.0, 2.0]).with_nulls(NullBuffer::new_valid(2)));
	}

	#[test]
	fn test_int1_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int1_with_bitvec([1, 2, 3], vec![true, false, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int1_with_bitvec([1, 2], vec![true, false]));
	}

	#[test]
	fn test_int2_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int2_with_bitvec([1, 2, 3, 4], vec![true, true, false, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int2([1, 2]).with_nulls(NullBuffer::new_valid(2)));
	}

	#[test]
	fn test_int4_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int4_with_bitvec([1, 2], vec![true, false]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int4([1]).with_nulls(NullBuffer::new_valid(1)));
	}

	#[test]
	fn test_int8_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int8_with_bitvec([1, 2, 3], vec![false, true, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int8_with_bitvec([1, 2], vec![false, true]));
	}

	#[test]
	fn test_int16_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int16_with_bitvec([1, 2], vec![true, true]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int16_with_bitvec([1], vec![true]));
	}

	#[test]
	fn test_uint1_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::uint1_with_bitvec([1, 2, 3], vec![false, false, true]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::uint1_with_bitvec([1, 2], vec![false, false]));
	}

	#[test]
	fn test_uint2_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::uint2_with_bitvec([1, 2], vec![true, false]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::uint2([1]).with_nulls(NullBuffer::new_valid(1)));
	}

	#[test]
	fn test_uint4_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::uint4_with_bitvec([10, 20], vec![false, true]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::uint4_with_bitvec([10], vec![false]));
	}

	#[test]
	fn test_uint8_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::uint8_with_bitvec([10, 20, 30], vec![true, true, false]),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::uint8([10, 20]).with_nulls(NullBuffer::new_valid(2)));
	}

	#[test]
	fn test_uint16_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::uint16_with_bitvec([100, 200, 300], vec![true, false, true]),
		)]);

		test_instance.take(1).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::uint16([100]).with_nulls(NullBuffer::new_valid(1)));
	}

	#[test]
	fn test_text_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"t",
			ColumnBuffer::utf8_with_bitvec(
				vec!["a".to_string(), "b".to_string(), "c".to_string()],
				vec![true, false, true],
			),
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(
			test_instance[0],
			ColumnBuffer::utf8_with_bitvec(["a".to_string(), "b".to_string()], vec![true, false])
		);
	}

	#[test]
	fn test_none_column() {
		let mut test_instance = Columns::new(vec![ColumnWithName::undefined_typed("u", ValueType::Boolean, 3)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0].len(), 2);
		assert_eq!(test_instance[0].get_value(0), Value::none_of(ValueType::Boolean));
		assert_eq!(test_instance[0].get_value(1), Value::none_of(ValueType::Boolean));
	}

	#[test]
	fn test_handles_none() {
		let mut test_instance = Columns::new(vec![ColumnWithName::undefined_typed("u", ValueType::Boolean, 5)]);

		test_instance.take(3).unwrap();

		assert_eq!(test_instance[0].len(), 3);
		assert_eq!(test_instance[0].get_value(0), Value::none_of(ValueType::Boolean));
	}

	#[test]
	fn test_n_larger_than_len_is_safe() {
		let mut test_instance = Columns::new(vec![ColumnWithName::new(
			"a",
			ColumnBuffer::int2_with_bitvec([10, 20], vec![true, false]),
		)]);

		test_instance.take(10).unwrap();

		assert_eq!(test_instance[0], ColumnBuffer::int2_with_bitvec([10, 20], vec![true, false]));
	}
}
