// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, layout::EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_u16(&self, row: &mut EncodedValues, index: usize, value: impl Into<u16>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint2);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut u16, value.into()) }
	}

	pub fn get_u16(&self, row: &EncodedValues, index: usize) -> u16 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint2);
		unsafe { (row.as_ptr().add(field.offset) as *const u16).read_unaligned() }
	}

	pub fn try_get_u16(&self, row: &EncodedValues, index: usize) -> Option<u16> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Uint2 {
			Some(self.get_u16(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::encoded::layout::EncodedValuesLayout;

	#[test]
	fn test_set_get_u16() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2]);
		let mut row = layout.allocate_for_testing();
		layout.set_u16(&mut row, 0, 65535u16);
		assert_eq!(layout.get_u16(&row, 0), 65535u16);
	}

	#[test]
	fn test_try_get_u16() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2]);
		let mut row = layout.allocate_for_testing();

		assert_eq!(layout.try_get_u16(&row, 0), None);

		layout.set_u16(&mut row, 0, 65535u16);
		assert_eq!(layout.try_get_u16(&row, 0), Some(65535u16));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2]);
		let mut row = layout.allocate_for_testing();

		layout.set_u16(&mut row, 0, u16::MAX);
		assert_eq!(layout.get_u16(&row, 0), u16::MAX);

		let mut row2 = layout.allocate_for_testing();
		layout.set_u16(&mut row2, 0, u16::MIN);
		assert_eq!(layout.get_u16(&row2, 0), u16::MIN);

		let mut row3 = layout.allocate_for_testing();
		layout.set_u16(&mut row3, 0, 0u16);
		assert_eq!(layout.get_u16(&row3, 0), 0u16);
	}

	#[test]
	fn test_various_values() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2]);

		let test_values = [0u16, 1u16, 255u16, 256u16, 32767u16, 32768u16, 65534u16, 65535u16];

		for value in test_values {
			let mut row = layout.allocate_for_testing();
			layout.set_u16(&mut row, 0, value);
			assert_eq!(layout.get_u16(&row, 0), value);
		}
	}

	#[test]
	fn test_port_numbers() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2]);

		// Test common port numbers
		let ports = [80u16, 443u16, 8080u16, 3000u16, 5432u16, 27017u16];

		for port in ports {
			let mut row = layout.allocate_for_testing();
			layout.set_u16(&mut row, 0, port);
			assert_eq!(layout.get_u16(&row, 0), port);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2, Type::Uint1, Type::Uint2]);
		let mut row = layout.allocate_for_testing();

		layout.set_u16(&mut row, 0, 60000u16);
		layout.set_u8(&mut row, 1, 200u8);
		layout.set_u16(&mut row, 2, 30000u16);

		assert_eq!(layout.get_u16(&row, 0), 60000u16);
		assert_eq!(layout.get_u8(&row, 1), 200u8);
		assert_eq!(layout.get_u16(&row, 2), 30000u16);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Uint2, Type::Uint2]);
		let mut row = layout.allocate_for_testing();

		layout.set_u16(&mut row, 0, 12345u16);

		assert_eq!(layout.try_get_u16(&row, 0), Some(12345));
		assert_eq!(layout.try_get_u16(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_u16(&row, 0), None);
	}

	#[test]
	fn test_try_get_u16_wrong_type() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_for_testing();

		layout.set_bool(&mut row, 0, true);

		assert_eq!(layout.try_get_u16(&row, 0), None);
	}
}
