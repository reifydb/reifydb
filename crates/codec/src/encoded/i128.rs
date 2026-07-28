// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_i128() {
		let shape = RowShape::testing(&[ValueType::Int16]);
		let mut row = shape.allocate();
		shape.set::<i128>(&mut row, 0, 123456789012345678901234567890i128);
		assert_eq!(shape.get::<i128>(&row, 0), 123456789012345678901234567890i128);
	}

	#[test]
	fn test_try_get_i128() {
		let shape = RowShape::testing(&[ValueType::Int16]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get::<i128>(&row, 0), None);

		shape.set::<i128>(&mut row, 0, 123456789012345678901234567890i128);
		assert_eq!(shape.try_get::<i128>(&row, 0), Some(123456789012345678901234567890i128));
	}

	#[test]
	fn test_extremes() {
		let shape = RowShape::testing(&[ValueType::Int16]);
		let mut row = shape.allocate();

		shape.set::<i128>(&mut row, 0, i128::MAX);
		assert_eq!(shape.get::<i128>(&row, 0), i128::MAX);

		let mut row2 = shape.allocate();
		shape.set::<i128>(&mut row2, 0, i128::MIN);
		assert_eq!(shape.get::<i128>(&row2, 0), i128::MIN);

		let mut row3 = shape.allocate();
		shape.set::<i128>(&mut row3, 0, 0i128);
		assert_eq!(shape.get::<i128>(&row3, 0), 0i128);
	}

	#[test]
	fn test_very_large_values() {
		let shape = RowShape::testing(&[ValueType::Int16]);

		let test_values = [
			-170141183460469231731687303715884105728i128, // i128::MIN
			-99999999999999999999999999999999999999i128,
			-1i128,
			0i128,
			1i128,
			99999999999999999999999999999999999999i128,
			170141183460469231731687303715884105727i128, // i128::MAX
		];

		for value in test_values {
			let mut row = shape.allocate();
			shape.set::<i128>(&mut row, 0, value);
			assert_eq!(shape.get::<i128>(&row, 0), value);
		}
	}

	#[test]
	fn test_powers_of_ten() {
		let shape = RowShape::testing(&[ValueType::Int16]);

		let powers = [
			1i128,
			10i128,
			100i128,
			1_000i128,
			10_000i128,
			100_000i128,
			1_000_000i128,
			10_000_000i128,
			100_000_000i128,
			1_000_000_000i128,
			10_000_000_000i128,
			100_000_000_000i128,
		];

		for power in powers {
			let mut row = shape.allocate();
			shape.set::<i128>(&mut row, 0, power);
			assert_eq!(shape.get::<i128>(&row, 0), power);

			let mut row2 = shape.allocate();
			shape.set::<i128>(&mut row2, 0, -power);
			assert_eq!(shape.get::<i128>(&row2, 0), -power);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Int16, ValueType::Boolean, ValueType::Int16]);
		let mut row = shape.allocate();

		let large_negative = -12345678901234567890123456789012345i128;
		let large_positive = 98765432109876543210987654321098765i128;

		shape.set::<i128>(&mut row, 0, large_negative);
		shape.set::<bool>(&mut row, 1, true);
		shape.set::<i128>(&mut row, 2, large_positive);

		assert_eq!(shape.get::<i128>(&row, 0), large_negative);
		assert_eq!(shape.get::<bool>(&row, 1), true);
		assert_eq!(shape.get::<i128>(&row, 2), large_positive);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Int16, ValueType::Int16]);
		let mut row = shape.allocate();

		let value = 170141183460469231731687303715884105727i128; // Max i128
		shape.set::<i128>(&mut row, 0, value);

		assert_eq!(shape.try_get::<i128>(&row, 0), Some(value));
		assert_eq!(shape.try_get::<i128>(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get::<i128>(&row, 0), None);
	}

	#[test]
	fn test_try_get_i128_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);

		assert_eq!(shape.try_get::<i128>(&row, 0), None);
	}
}
