// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{f32, ptr};

use reifydb_type::value::r#type::Type;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_f32(&self, row: &mut EncodedRow, index: usize, value: impl Into<f32>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Float4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut f32,
				value.into(),
			)
		}
	}

	pub fn get_f32(&self, row: &EncodedRow, index: usize) -> f32 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Float4);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const f32).read_unaligned() }
	}

	pub fn try_get_f32(&self, row: &EncodedRow, index: usize) -> Option<f32> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Float4 {
			Some(self.get_f32(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use std::f32::consts::{E, PI};

	use reifydb_type::value::r#type::Type;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_f32() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();
		shape.set_f32(&mut row, 0, 1.25f32);
		assert_eq!(shape.get_f32(&row, 0), 1.25f32);
	}

	#[test]
	fn test_try_get_f32() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_f32(&row, 0), None);

		shape.set_f32(&mut row, 0, 1.25f32);
		assert_eq!(shape.try_get_f32(&row, 0), Some(1.25f32));
	}

	#[test]
	fn test_special_values() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		// Test zero
		shape.set_f32(&mut row, 0, 0.0f32);
		assert_eq!(shape.get_f32(&row, 0), 0.0f32);

		// Test negative zero
		let mut row2 = shape.allocate();
		shape.set_f32(&mut row2, 0, -0.0f32);
		assert_eq!(shape.get_f32(&row2, 0), -0.0f32);

		// Test infinity
		let mut row3 = shape.allocate();
		shape.set_f32(&mut row3, 0, f32::INFINITY);
		assert_eq!(shape.get_f32(&row3, 0), f32::INFINITY);

		// Test negative infinity
		let mut row4 = shape.allocate();
		shape.set_f32(&mut row4, 0, f32::NEG_INFINITY);
		assert_eq!(shape.get_f32(&row4, 0), f32::NEG_INFINITY);

		// Test NaN
		let mut row5 = shape.allocate();
		shape.set_f32(&mut row5, 0, f32::NAN);
		assert!(shape.get_f32(&row5, 0).is_nan());
	}

	#[test]
	fn test_extreme_values() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		shape.set_f32(&mut row, 0, f32::MAX);
		assert_eq!(shape.get_f32(&row, 0), f32::MAX);

		let mut row2 = shape.allocate();
		shape.set_f32(&mut row2, 0, f32::MIN);
		assert_eq!(shape.get_f32(&row2, 0), f32::MIN);

		let mut row3 = shape.allocate();
		shape.set_f32(&mut row3, 0, f32::MIN_POSITIVE);
		assert_eq!(shape.get_f32(&row3, 0), f32::MIN_POSITIVE);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[Type::Float4, Type::Int4, Type::Float4]);
		let mut row = shape.allocate();

		shape.set_f32(&mut row, 0, 3.14f32);
		shape.set_i32(&mut row, 1, 42);
		shape.set_f32(&mut row, 2, -2.718f32);

		assert_eq!(shape.get_f32(&row, 0), 3.14f32);
		assert_eq!(shape.get_i32(&row, 1), 42);
		assert_eq!(shape.get_f32(&row, 2), -2.718f32);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[Type::Float4, Type::Float4]);
		let mut row = shape.allocate();

		shape.set_f32(&mut row, 0, 3.14f32);

		assert_eq!(shape.try_get_f32(&row, 0), Some(3.14f32));
		assert_eq!(shape.try_get_f32(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_f32(&row, 0), None);
	}

	#[test]
	fn test_try_get_f32_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_f32(&row, 0), None);
	}

	#[test]
	fn test_subnormal_values() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		// Test smallest positive subnormal
		let min_subnormal = f32::from_bits(0x00000001);
		shape.set_f32(&mut row, 0, min_subnormal);
		assert_eq!(shape.get_f32(&row, 0).to_bits(), min_subnormal.to_bits());

		// Test largest subnormal (just below MIN_POSITIVE)
		let max_subnormal = f32::from_bits(0x007fffff);
		shape.set_f32(&mut row, 0, max_subnormal);
		assert_eq!(shape.get_f32(&row, 0).to_bits(), max_subnormal.to_bits());

		// Test negative subnormals
		let neg_subnormal = f32::from_bits(0x80000001);
		shape.set_f32(&mut row, 0, neg_subnormal);
		assert_eq!(shape.get_f32(&row, 0).to_bits(), neg_subnormal.to_bits());
	}

	#[test]
	fn test_nan_payload_preservation() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		// Test different NaN representations
		let quiet_nan = f32::NAN;
		shape.set_f32(&mut row, 0, quiet_nan);
		assert!(shape.get_f32(&row, 0).is_nan());

		// Test NaN with specific payload
		let nan_with_payload = f32::from_bits(0x7fc00001);
		shape.set_f32(&mut row, 0, nan_with_payload);
		assert_eq!(shape.get_f32(&row, 0).to_bits(), nan_with_payload.to_bits());

		// Test negative NaN
		let neg_nan = f32::from_bits(0xffc00000);
		shape.set_f32(&mut row, 0, neg_nan);
		assert_eq!(shape.get_f32(&row, 0).to_bits(), neg_nan.to_bits());
	}

	#[test]
	fn test_repeated_operations() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();
		let initial_len = row.len();

		// Set same field many times with different values
		for i in 0..1000 {
			let value = (i as f32) * 0.1;
			shape.set_f32(&mut row, 0, value);
			assert_eq!(shape.get_f32(&row, 0), value);
		}

		// Size shouldn't grow for static type
		assert_eq!(row.len(), initial_len);
	}

	#[test]
	fn test_unaligned_access() {
		let shape = create_unaligned_layout(Type::Float4);
		let mut row = shape.allocate();

		// Test at odd offset (index 1)
		shape.set_f32(&mut row, 1, PI);
		assert_eq!(shape.get_f32(&row, 1), PI);

		// Test at another odd offset (index 3)
		shape.set_f32(&mut row, 3, E);
		assert_eq!(shape.get_f32(&row, 3), E);

		// Verify both values are preserved
		assert_eq!(shape.get_f32(&row, 1), PI);
		assert_eq!(shape.get_f32(&row, 3), E);
	}

	#[test]
	fn test_denormalized_transitions() {
		let shape = RowShape::testing(&[Type::Float4]);
		let mut row = shape.allocate();

		// Test transition from normal to subnormal
		let values = [
			f32::MIN_POSITIVE,       // Smallest normal
			f32::MIN_POSITIVE / 2.0, // Becomes subnormal
			f32::MIN_POSITIVE / 4.0, // Smaller subnormal
			0.0f32,                  // Underflows to zero
		];

		for value in values {
			shape.set_f32(&mut row, 0, value);
			let retrieved = shape.get_f32(&row, 0);
			if value == 0.0 {
				assert_eq!(retrieved, 0.0);
			} else {
				// For subnormals, compare bits to ensure exact
				// preservation
				assert_eq!(retrieved.to_bits(), value.to_bits());
			}
		}
	}

	/// Creates a layout with odd alignment to test unaligned access
	pub fn create_unaligned_layout(target_type: Type) -> RowShape {
		// Use Int1 (1 byte) to create odd alignment
		RowShape::testing(&[
			Type::Int1,          // 1 byte offset
			target_type.clone(), // Now at odd offset
			Type::Int1,          // Another odd-sized field
			target_type,         /* Another instance at different odd
			                      * offset */
		])
	}
}
