// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{value_type::ValueType, vector::VectorValue},
};

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_vector(&self, row: &mut EncodedRow, index: usize, value: &VectorValue) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert!(matches!(self.fields()[index].constraint.get_type().inner_type(), ValueType::Vector(_)));
		}
		self.replace_dynamic_data(row, index, &value.to_le_bytes());
	}

	pub fn get_vector(&self, row: &EncodedRow, index: usize) -> VectorValue {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert!(matches!(field.constraint.get_type().inner_type(), ValueType::Vector(_)));
		}

		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let vector_start = dynamic_start + offset;
		let vector_slice = &row.as_slice()[vector_start..vector_start + length];

		VectorValue::from_le_bytes(vector_slice)
	}

	pub fn try_get_vector(&self, row: &EncodedRow, index: usize) -> Option<VectorValue> {
		if row.is_defined(index) && matches!(self.fields()[index].constraint.get_type(), ValueType::Vector(_)) {
			Some(self.get_vector(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::{value_type::ValueType, vector::VectorValue};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_vector() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		let vector = VectorValue::new(vec![0.1, 0.2, 0.3, 0.4]);
		shape.set_vector(&mut row, 0, &vector);
		assert_eq!(shape.get_vector(&row, 0), vector);
	}

	#[test]
	fn test_try_get_vector() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_vector(&row, 0), None);

		let vector = VectorValue::new(vec![1.0, -2.0]);
		shape.set_vector(&mut row, 0, &vector);
		assert_eq!(shape.try_get_vector(&row, 0), Some(vector));
	}

	#[test]
	fn test_payload_is_four_bytes_per_element() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		shape.set_vector(&mut row, 0, &VectorValue::new(vec![1.0, 2.0, 3.0]));
		assert_eq!(row.len(), shape.total_static_size() + 12);
	}

	#[test]
	fn test_round_trip_preserves_element_order() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		let vector = VectorValue::new(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
		shape.set_vector(&mut row, 0, &vector);
		assert_eq!(shape.get_vector(&row, 0).as_slice(), &[1.0f32, 2.0, 3.0, 4.0, 5.0]);
	}

	#[test]
	fn test_high_dimension() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		let data: Vec<f32> = (0..1536).map(|i| i as f32 * 0.001).collect();
		let vector = VectorValue::new(data);
		shape.set_vector(&mut row, 0, &vector);
		assert_eq!(shape.get_vector(&row, 0), vector);
		assert_eq!(shape.get_vector(&row, 0).dims(), 1536);
	}

	#[test]
	fn test_update_vector_to_different_dimension() {
		let shape = RowShape::testing(&[ValueType::Vector(4)]);
		let mut row = shape.allocate();

		let long = VectorValue::new(vec![1.0, 2.0, 3.0, 4.0]);
		shape.set_vector(&mut row, 0, &long);
		assert_eq!(shape.get_vector(&row, 0), long);

		let short = VectorValue::new(vec![9.0]);
		shape.set_vector(&mut row, 0, &short);
		assert_eq!(shape.get_vector(&row, 0), short);
		assert_eq!(row.len(), shape.total_static_size() + 4);
	}

	#[test]
	fn test_mixed_with_static_and_other_dynamic_fields() {
		let shape = RowShape::testing(&[
			ValueType::Boolean,
			ValueType::Vector(2),
			ValueType::Utf8,
			ValueType::Vector(3),
		]);
		let mut row = shape.allocate();

		let first = VectorValue::new(vec![0.5, 0.25]);
		let second = VectorValue::new(vec![-1.0, 0.0, 1.0]);

		shape.set_bool(&mut row, 0, true);
		shape.set_vector(&mut row, 1, &first);
		shape.set_utf8(&mut row, 2, "hello");
		shape.set_vector(&mut row, 3, &second);

		assert_eq!(shape.get_bool(&row, 0), true);
		assert_eq!(shape.get_vector(&row, 1), first);
		assert_eq!(shape.get_utf8(&row, 2), "hello");
		assert_eq!(shape.get_vector(&row, 3), second);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Vector(2), ValueType::Vector(2)]);
		let mut row = shape.allocate();

		let vector = VectorValue::new(vec![1.0, 2.0]);
		shape.set_vector(&mut row, 0, &vector);

		assert_eq!(shape.try_get_vector(&row, 0), Some(vector));
		assert_eq!(shape.try_get_vector(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_vector(&row, 0), None);
	}

	#[test]
	fn test_try_get_vector_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_vector(&row, 0), None);
	}
}
