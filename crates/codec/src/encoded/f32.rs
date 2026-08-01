// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use std::f32::consts::{E, PI};

	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_f32() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();
		shape.set::<f32>(&mut row, 0, 1.25f32);
		assert_eq!(shape.get::<f32>(&row, 0), 1.25f32);
	}

	#[test]
	fn test_try_get_f32() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get::<f32>(&row, 0), None);

		shape.set::<f32>(&mut row, 0, 1.25f32);
		assert_eq!(shape.try_get::<f32>(&row, 0), Some(1.25f32));
	}

	#[test]
	fn test_special_values() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		shape.set::<f32>(&mut row, 0, 0.0f32);
		assert_eq!(shape.get::<f32>(&row, 0), 0.0f32);

		let mut row2 = shape.allocate();
		shape.set::<f32>(&mut row2, 0, -0.0f32);
		assert_eq!(shape.get::<f32>(&row2, 0), -0.0f32);

		let mut row3 = shape.allocate();
		shape.set::<f32>(&mut row3, 0, f32::INFINITY);
		assert_eq!(shape.get::<f32>(&row3, 0), f32::INFINITY);

		let mut row4 = shape.allocate();
		shape.set::<f32>(&mut row4, 0, f32::NEG_INFINITY);
		assert_eq!(shape.get::<f32>(&row4, 0), f32::NEG_INFINITY);

		let mut row5 = shape.allocate();
		shape.set::<f32>(&mut row5, 0, f32::NAN);
		assert!(shape.get::<f32>(&row5, 0).is_nan());
	}

	#[test]
	fn test_extreme_values() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		shape.set::<f32>(&mut row, 0, f32::MAX);
		assert_eq!(shape.get::<f32>(&row, 0), f32::MAX);

		let mut row2 = shape.allocate();
		shape.set::<f32>(&mut row2, 0, f32::MIN);
		assert_eq!(shape.get::<f32>(&row2, 0), f32::MIN);

		let mut row3 = shape.allocate();
		shape.set::<f32>(&mut row3, 0, f32::MIN_POSITIVE);
		assert_eq!(shape.get::<f32>(&row3, 0), f32::MIN_POSITIVE);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Float4, ValueType::Int4, ValueType::Float4]);
		let mut row = shape.allocate();

		shape.set::<f32>(&mut row, 0, 3.14f32);
		shape.set::<i32>(&mut row, 1, 42i32);
		shape.set::<f32>(&mut row, 2, -2.718f32);

		assert_eq!(shape.get::<f32>(&row, 0), 3.14f32);
		assert_eq!(shape.get::<i32>(&row, 1), 42);
		assert_eq!(shape.get::<f32>(&row, 2), -2.718f32);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Float4, ValueType::Float4]);
		let mut row = shape.allocate();

		shape.set::<f32>(&mut row, 0, 3.14f32);

		assert_eq!(shape.try_get::<f32>(&row, 0), Some(3.14f32));
		assert_eq!(shape.try_get::<f32>(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get::<f32>(&row, 0), None);
	}

	#[test]
	fn test_try_get_f32_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);

		assert_eq!(shape.try_get::<f32>(&row, 0), None);
	}

	#[test]
	fn test_subnormal_values() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		// Bit comparison, not value comparison: subnormals must survive the slot bit-exact.
		let min_subnormal = f32::from_bits(0x00000001);
		shape.set::<f32>(&mut row, 0, min_subnormal);
		assert_eq!(shape.get::<f32>(&row, 0).to_bits(), min_subnormal.to_bits());

		let max_subnormal = f32::from_bits(0x007fffff);
		shape.set::<f32>(&mut row, 0, max_subnormal);
		assert_eq!(shape.get::<f32>(&row, 0).to_bits(), max_subnormal.to_bits());

		let neg_subnormal = f32::from_bits(0x80000001);
		shape.set::<f32>(&mut row, 0, neg_subnormal);
		assert_eq!(shape.get::<f32>(&row, 0).to_bits(), neg_subnormal.to_bits());
	}

	#[test]
	fn test_nan_payload_preservation() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		// The slot stores raw bits, so the NaN payload and sign must come back unchanged.
		let quiet_nan = f32::NAN;
		shape.set::<f32>(&mut row, 0, quiet_nan);
		assert!(shape.get::<f32>(&row, 0).is_nan());

		let nan_with_payload = f32::from_bits(0x7fc00001);
		shape.set::<f32>(&mut row, 0, nan_with_payload);
		assert_eq!(shape.get::<f32>(&row, 0).to_bits(), nan_with_payload.to_bits());

		let neg_nan = f32::from_bits(0xffc00000);
		shape.set::<f32>(&mut row, 0, neg_nan);
		assert_eq!(shape.get::<f32>(&row, 0).to_bits(), neg_nan.to_bits());
	}

	#[test]
	fn test_repeated_operations() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();
		let initial_len = row.len();

		for i in 0..1000 {
			let value = (i as f32) * 0.1;
			shape.set::<f32>(&mut row, 0, value);
			assert_eq!(shape.get::<f32>(&row, 0), value);
		}

		// A static-width field is overwritten in place, so repeated writes must not grow the row.
		assert_eq!(row.len(), initial_len);
	}

	#[test]
	fn test_unaligned_access() {
		let shape = create_unaligned_layout(ValueType::Float4);
		let mut row = shape.allocate();

		shape.set::<f32>(&mut row, 1, PI);
		assert_eq!(shape.get::<f32>(&row, 1), PI);

		shape.set::<f32>(&mut row, 3, E);
		assert_eq!(shape.get::<f32>(&row, 3), E);

		assert_eq!(shape.get::<f32>(&row, 1), PI);
		assert_eq!(shape.get::<f32>(&row, 3), E);
	}

	#[test]
	fn test_denormalized_transitions() {
		let shape = RowShape::testing(&[ValueType::Float4]);
		let mut row = shape.allocate();

		let values = [
			f32::MIN_POSITIVE,       // Smallest normal
			f32::MIN_POSITIVE / 2.0, // Becomes subnormal
			f32::MIN_POSITIVE / 4.0, // Smaller subnormal
			0.0f32,                  // Underflows to zero
		];

		for value in values {
			shape.set::<f32>(&mut row, 0, value);
			let retrieved = shape.get::<f32>(&row, 0);
			if value == 0.0 {
				assert_eq!(retrieved, 0.0);
			} else {
				assert_eq!(retrieved.to_bits(), value.to_bits());
			}
		}
	}

	/// Interleaves 1-byte fields so the target type never lands on its natural alignment.
	pub fn create_unaligned_layout(target_type: ValueType) -> RowShape {
		RowShape::testing(&[
			ValueType::Int1,     // 1 byte offset
			target_type.clone(), // Now at odd offset
			ValueType::Int1,     // Another odd-sized field
			target_type,         /* Another instance at different odd
			                      * offset */
		])
	}
}
