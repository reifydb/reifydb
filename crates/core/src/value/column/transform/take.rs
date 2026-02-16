// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::value::column::columns::Columns;

impl Columns {
	pub fn take(&mut self, n: usize) -> reifydb_type::Result<()> {
		// Take the first n encoded numbers
		if !self.row_numbers.is_empty() {
			let actual_n = n.min(self.row_numbers.len());
			let new_row_numbers: Vec<_> = self.row_numbers.iter().take(actual_n).copied().collect();
			self.row_numbers = CowVec::new(new_row_numbers);
		}

		// Take the first n rows from columns
		let mut columns = Vec::with_capacity(self.len());

		for col in self.iter() {
			let data = col.data().take(n);
			columns.push(col.with_new_data(data));
		}

		self.columns = CowVec::new(columns);

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::value::column::{Column, ColumnData};

	#[test]
	fn test_bool_column() {
		let mut test_instance =
			Columns::new(vec![Column::bool_with_bitvec("flag", [true, true, false], [false, true, true])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::bool_with_bitvec([true], [false]));
	}

	#[test]
	fn test_float4_column() {
		let mut test_instance =
			Columns::new(vec![Column::float4_with_bitvec("a", [1.0, 2.0, 3.0], [true, false, true])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::float4_with_bitvec([1.0, 2.0], [true, false]));
	}

	#[test]
	fn test_float8_column() {
		let mut test_instance = Columns::new(vec![Column::float8_with_bitvec(
			"a",
			[1f64, 2.0, 3.0, 4.0],
			[true, true, false, true],
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::float8_with_bitvec([1.0, 2.0], [true, true]));
	}

	#[test]
	fn test_int1_column() {
		let mut test_instance =
			Columns::new(vec![Column::int1_with_bitvec("a", [1, 2, 3], [true, false, true])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int1_with_bitvec([1, 2], [true, false]));
	}

	#[test]
	fn test_int2_column() {
		let mut test_instance =
			Columns::new(vec![Column::int2_with_bitvec("a", [1, 2, 3, 4], [true, true, false, true])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([1, 2], [true, true]));
	}

	#[test]
	fn test_int4_column() {
		let mut test_instance = Columns::new(vec![Column::int4_with_bitvec("a", [1, 2], [true, false])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int4_with_bitvec([1], [true]));
	}

	#[test]
	fn test_int8_column() {
		let mut test_instance =
			Columns::new(vec![Column::int8_with_bitvec("a", [1, 2, 3], [false, true, true])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int8_with_bitvec([1, 2], [false, true]));
	}

	#[test]
	fn test_int16_column() {
		let mut test_instance = Columns::new(vec![Column::int16_with_bitvec("a", [1, 2], [true, true])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int16_with_bitvec([1], [true]));
	}

	#[test]
	fn test_uint1_column() {
		let mut test_instance =
			Columns::new(vec![Column::uint1_with_bitvec("a", [1, 2, 3], [false, false, true])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::uint1_with_bitvec([1, 2], [false, false]));
	}

	#[test]
	fn test_uint2_column() {
		let mut test_instance = Columns::new(vec![Column::uint2_with_bitvec("a", [1, 2], [true, false])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::uint2_with_bitvec([1], [true]));
	}

	#[test]
	fn test_uint4_column() {
		let mut test_instance = Columns::new(vec![Column::uint4_with_bitvec("a", [10, 20], [false, true])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::uint4_with_bitvec([10], [false]));
	}

	#[test]
	fn test_uint8_column() {
		let mut test_instance =
			Columns::new(vec![Column::uint8_with_bitvec("a", [10, 20, 30], [true, true, false])]);

		test_instance.take(2).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::uint8_with_bitvec([10, 20], [true, true]));
	}

	#[test]
	fn test_uint16_column() {
		let mut test_instance =
			Columns::new(vec![Column::uint16_with_bitvec("a", [100, 200, 300], [true, false, true])]);

		test_instance.take(1).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::uint16_with_bitvec([100], [true]));
	}

	#[test]
	fn test_text_column() {
		let mut test_instance = Columns::new(vec![Column::utf8_with_bitvec(
			"t",
			vec!["a".to_string(), "b".to_string(), "c".to_string()],
			vec![true, false, true],
		)]);

		test_instance.take(2).unwrap();

		assert_eq!(
			*test_instance[0].data(),
			ColumnData::utf8_with_bitvec(["a".to_string(), "b".to_string()], [true, false])
		);
	}

	#[test]
	fn test_none_column() {
		let mut test_instance = Columns::new(vec![Column::undefined_typed("u", Type::Boolean, 3)]);

		test_instance.take(2).unwrap();

		assert_eq!(test_instance[0].data().len(), 2);
		assert_eq!(test_instance[0].data().get_value(0), reifydb_type::value::Value::None);
		assert_eq!(test_instance[0].data().get_value(1), reifydb_type::value::Value::None);
	}

	#[test]
	fn test_handles_none() {
		let mut test_instance = Columns::new(vec![Column::undefined_typed("u", Type::Boolean, 5)]);

		test_instance.take(3).unwrap();

		assert_eq!(test_instance[0].data().len(), 3);
		assert_eq!(test_instance[0].data().get_value(0), reifydb_type::value::Value::None);
	}

	#[test]
	fn test_n_larger_than_len_is_safe() {
		let mut test_instance = Columns::new(vec![Column::int2_with_bitvec("a", [10, 20], [true, false])]);

		test_instance.take(10).unwrap();

		assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([10, 20], [true, false]));
	}
}
