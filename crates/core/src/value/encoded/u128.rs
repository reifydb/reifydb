// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_u128(&self, row: &mut EncodedValues, index: usize, value: impl Into<u128>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint16);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut u128, value.into())
		}
	}

	pub fn get_u128(&self, row: &EncodedValues, index: usize) -> u128 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Uint16);
		unsafe { (row.as_ptr().add(field.offset) as *const u128).read_unaligned() }
	}

	pub fn try_get_u128(&self, row: &EncodedValues, index: usize) -> Option<u128> {
		if row.is_defined(index) {
			Some(self.get_u128(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_set_get_u128() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);
		let mut row = layout.allocate();
		layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
		assert_eq!(layout.get_u128(&row, 0), 340282366920938463463374607431768211455u128);
	}

	#[test]
	fn test_try_get_u128() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);
		let mut row = layout.allocate();

		assert_eq!(layout.try_get_u128(&row, 0), None);

		layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
		assert_eq!(layout.try_get_u128(&row, 0), Some(340282366920938463463374607431768211455u128));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);
		let mut row = layout.allocate();

		layout.set_u128(&mut row, 0, u128::MAX);
		assert_eq!(layout.get_u128(&row, 0), u128::MAX);

		let mut row2 = layout.allocate();
		layout.set_u128(&mut row2, 0, u128::MIN);
		assert_eq!(layout.get_u128(&row2, 0), u128::MIN);

		let mut row3 = layout.allocate();
		layout.set_u128(&mut row3, 0, 0u128);
		assert_eq!(layout.get_u128(&row3, 0), 0u128);
	}

	#[test]
	fn test_very_large_values() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);

		let test_values = [
			0u128,
			1u128,
			99999999999999999999999999999999999999u128,
			170141183460469231731687303715884105727u128, // i128::MAX as u128
			170141183460469231731687303715884105728u128, // i128::MAX + 1
			300000000000000000000000000000000000000u128,
			340282366920938463463374607431768211454u128,
			340282366920938463463374607431768211455u128, // u128::MAX
		];

		for value in test_values {
			let mut row = layout.allocate();
			layout.set_u128(&mut row, 0, value);
			assert_eq!(layout.get_u128(&row, 0), value);
		}
	}

	#[test]
	fn test_powers_of_two() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);

		let powers = [
			1u128, 2u128, 4u128, 8u128, 16u128, 32u128, 64u128, 128u128, 256u128, 512u128, 1024u128,
			2048u128, 4096u128, 8192u128, 16384u128, 32768u128, 65536u128,
		];

		for power in powers {
			let mut row = layout.allocate();
			layout.set_u128(&mut row, 0, power);
			assert_eq!(layout.get_u128(&row, 0), power);
		}
	}

	#[test]
	fn test_ipv6_addresses() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);

		// Test values representing IPv6 addresses as u128
		let ipv6_values = [
			0u128,                                       // ::0
			1u128,                                       // ::1 (loopback)
			281470681743360u128,                         // ::ffff:0:0 (IPv4-mapped prefix)
			338953138925153547590470800371487866880u128, // Example IPv6
		];

		for ipv6 in ipv6_values {
			let mut row = layout.allocate();
			layout.set_u128(&mut row, 0, ipv6);
			assert_eq!(layout.get_u128(&row, 0), ipv6);
		}
	}

	#[test]
	fn test_uuid_values() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16]);

		// Test values that could represent UUIDs as u128
		let uuid_values = [
			123456789012345678901234567890123456789u128,
			123456789012345678901234567890123456789u128,
			111111111111111111111111111111111111111u128,
		];

		for uuid_val in uuid_values {
			let mut row = layout.allocate();
			layout.set_u128(&mut row, 0, uuid_val);
			assert_eq!(layout.get_u128(&row, 0), uuid_val);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16, Type::Boolean, Type::Uint16]);
		let mut row = layout.allocate();

		let large_value1 = 200000000000000000000000000000000000000u128;
		let large_value2 = 150000000000000000000000000000000000000u128;

		layout.set_u128(&mut row, 0, large_value1);
		layout.set_bool(&mut row, 1, true);
		layout.set_u128(&mut row, 2, large_value2);

		assert_eq!(layout.get_u128(&row, 0), large_value1);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_u128(&row, 2), large_value2);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Uint16, Type::Uint16]);
		let mut row = layout.allocate();

		let value = 340282366920938463463374607431768211455u128;
		layout.set_u128(&mut row, 0, value);

		assert_eq!(layout.try_get_u128(&row, 0), Some(value));
		assert_eq!(layout.try_get_u128(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_u128(&row, 0), None);
	}
}
