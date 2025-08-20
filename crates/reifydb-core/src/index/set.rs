// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use uuid::Uuid;

use crate::{
	SortDirection, Type,
	index::{EncodedIndexKey, EncodedIndexLayout},
	value::{Date, DateTime, IdentityId, Interval, Time, Uuid4, Uuid7},
};

impl EncodedIndexLayout {
	pub fn set_bool(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<bool>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Bool);
		key.set_valid(index, true);

		let byte_value = match field.direction {
			SortDirection::Asc => {
				if value.into() {
					1u8
				} else {
					0u8
				}
			}
			SortDirection::Desc => {
				if value.into() {
					0u8
				} else {
					1u8
				}
			}
		};

		unsafe {
			ptr::write_unaligned(
				key.make_mut().as_mut_ptr().add(field.offset),
				byte_value,
			)
		}
	}

	pub fn set_f32(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<f32>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Float4);
		key.set_valid(index, true);

		let v = value.into();
		let mut bytes = v.to_bits().to_be_bytes();

		// Apply ASC encoding first
		if v.is_sign_negative() {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		} else {
			bytes[0] ^= 0x80;
		}

		// For DESC, invert all bytes
		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				4,
			);
		}
	}

	pub fn set_f64(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<f64>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Float8);
		key.set_valid(index, true);

		let v = value.into();
		let mut bytes = v.to_bits().to_be_bytes();

		// Apply ASC encoding first
		if v.is_sign_negative() {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		} else {
			bytes[0] ^= 0x80;
		}

		// For DESC, invert all bytes
		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
		}
	}

	pub fn set_i8(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<i8>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int1);
		key.set_valid(index, true);

		let mut bytes = value.into().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::write_unaligned(
				key.make_mut().as_mut_ptr().add(field.offset),
				bytes[0],
			)
		}
	}

	pub fn set_i16(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<i16>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int2);
		key.set_valid(index, true);

		let mut bytes = value.into().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				2,
			);
		}
	}

	pub fn set_i32(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<i32>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int4);
		key.set_valid(index, true);

		let mut bytes = value.into().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				4,
			);
		}
	}

	pub fn set_i64(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<i64>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int8);
		key.set_valid(index, true);

		let mut bytes = value.into().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
		}
	}

	pub fn set_i128(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<i128>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int16);
		key.set_valid(index, true);

		let mut bytes = value.into().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_u8(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u8>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint1);
		key.set_valid(index, true);

		let byte = match field.direction {
			SortDirection::Asc => value.into(),
			SortDirection::Desc => !value.into(),
		};

		unsafe {
			ptr::write_unaligned(
				key.make_mut().as_mut_ptr().add(field.offset),
				byte,
			)
		}
	}

	pub fn set_u16(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u16>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint2);
		key.set_valid(index, true);

		let bytes = match field.direction {
			SortDirection::Asc => value.into().to_be_bytes(),
			SortDirection::Desc => (!value.into()).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				2,
			);
		}
	}

	pub fn set_u32(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u32>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint4);
		key.set_valid(index, true);

		let bytes = match field.direction {
			SortDirection::Asc => value.into().to_be_bytes(),
			SortDirection::Desc => (!value.into()).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				4,
			);
		}
	}

	pub fn set_u64(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u64>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint8);
		key.set_valid(index, true);

		let bytes = match field.direction {
			SortDirection::Asc => value.into().to_be_bytes(),
			SortDirection::Desc => (!value.into()).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
		}
	}

	pub fn set_u128(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u128>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint16);
		key.set_valid(index, true);

		let bytes = match field.direction {
			SortDirection::Asc => value.into().to_be_bytes(),
			SortDirection::Desc => (!value.into()).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_row_number(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: impl Into<u64>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::RowNumber);
		key.set_valid(index, true);

		let bytes = match field.direction {
			SortDirection::Asc => value.into().to_be_bytes(),
			SortDirection::Desc => (!value.into()).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
		}
	}

	pub fn set_date(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: Date,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Date);
		key.set_valid(index, true);

		let days = value.to_days_since_epoch();
		let mut bytes = days.to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				bytes[0] ^= 0x80;
				for b in bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				4,
			);
		}
	}

	pub fn set_datetime(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: DateTime,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::DateTime);
		key.set_valid(index, true);

		let (seconds, nanos) = value.to_parts();
		let mut sec_bytes = seconds.to_be_bytes();
		let mut nano_bytes = nanos.to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				sec_bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				sec_bytes[0] ^= 0x80;
				for b in sec_bytes.iter_mut() {
					*b = !*b;
				}
				for b in nano_bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				sec_bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
			ptr::copy_nonoverlapping(
				nano_bytes.as_ptr(),
				key.make_mut()
					.as_mut_ptr()
					.add(field.offset + 8),
				4,
			);
		}
	}

	pub fn set_time(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: Time,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Time);
		key.set_valid(index, true);

		let nanos = value.to_nanos_since_midnight();
		let bytes = match field.direction {
			SortDirection::Asc => nanos.to_be_bytes(),
			SortDirection::Desc => (!nanos).to_be_bytes(),
		};

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				8,
			);
		}
	}

	pub fn set_interval(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: Interval,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Interval);
		key.set_valid(index, true);

		let mut months_bytes = value.get_months().to_be_bytes();
		let mut days_bytes = value.get_days().to_be_bytes();
		let mut nanos_bytes = value.get_nanos().to_be_bytes();

		match field.direction {
			SortDirection::Asc => {
				months_bytes[0] ^= 0x80;
				days_bytes[0] ^= 0x80;
				nanos_bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				months_bytes[0] ^= 0x80;
				days_bytes[0] ^= 0x80;
				nanos_bytes[0] ^= 0x80;
				for b in months_bytes.iter_mut() {
					*b = !*b;
				}
				for b in days_bytes.iter_mut() {
					*b = !*b;
				}
				for b in nanos_bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				months_bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				4,
			);
			ptr::copy_nonoverlapping(
				days_bytes.as_ptr(),
				key.make_mut()
					.as_mut_ptr()
					.add(field.offset + 4),
				4,
			);
			ptr::copy_nonoverlapping(
				nanos_bytes.as_ptr(),
				key.make_mut()
					.as_mut_ptr()
					.add(field.offset + 8),
				8,
			);
		}
	}

	pub fn set_uuid4(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: Uuid4,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uuid4);
		key.set_valid(index, true);

		let uuid: Uuid = value.into();
		let uuid_bytes = uuid.as_bytes();
		let mut bytes = [0u8; 16];
		bytes.copy_from_slice(uuid_bytes);

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_uuid7(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: Uuid7,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uuid7);
		key.set_valid(index, true);

		let uuid: Uuid = value.into();
		let uuid_bytes = uuid.as_bytes();
		let mut bytes = [0u8; 16];
		bytes.copy_from_slice(uuid_bytes);

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_identity_id(
		&self,
		key: &mut EncodedIndexKey,
		index: usize,
		value: IdentityId,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::IdentityId);
		key.set_valid(index, true);

		// Direct conversion from inner Uuid7 to Uuid
		let uuid: Uuid = value.0.into();
		let uuid_bytes = uuid.as_bytes();
		let mut bytes = [0u8; 16];
		bytes.copy_from_slice(uuid_bytes);

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		unsafe {
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				key.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_undefined(&self, key: &mut EncodedIndexKey, index: usize) {
		let field = &self.fields[index];
		key.set_valid(index, false);

		let buf = key.make_mut();
		let start = field.offset;
		let end = start + field.size;
		buf[start..end].fill(0);
	}
}

#[cfg(test)]
mod tests {
	use crate::{SortDirection, Type, index::EncodedIndexLayout};

	mod bool {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Bool],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key_false = layout.allocate_key();
			let mut key_true = layout.allocate_key();

			layout.set_bool(&mut key_false, 0, false);
			layout.set_bool(&mut key_true, 0, true);

			// Check bitvec shows field is set
			assert_eq!(key_false[0] & 0x01, 0x01);
			assert_eq!(key_true[0] & 0x01, 0x01);

			// Check values at field offset (after bitvec)
			let offset = layout.fields[0].offset;
			assert_eq!(key_false[offset], 0);
			assert_eq!(key_true[offset], 1);

			// Verify ordering
			assert!(key_false.as_slice() < key_true.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Bool],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key_false = layout.allocate_key();
			let mut key_true = layout.allocate_key();

			layout.set_bool(&mut key_false, 0, false);
			layout.set_bool(&mut key_true, 0, true);

			// Check values at field offset (inverted for DESC)
			let offset = layout.fields[0].offset;
			assert_eq!(key_false[offset], 1); // false becomes 1 in DESC
			assert_eq!(key_true[offset], 0); // true becomes 0 in DESC

			// Verify ordering (reversed)
			assert!(key_false.as_slice() > key_true.as_slice());
		}
	}

	mod i8 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int1],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_zero = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_i8(&mut key_neg, 0, -128i8);
			layout.set_i8(&mut key_zero, 0, 0i8);
			layout.set_i8(&mut key_pos, 0, 127i8);

			let offset = layout.fields[0].offset;
			// -128 with sign bit flipped: 0x80 -> 0x00
			assert_eq!(key_neg[offset], 0x00);
			// 0 with sign bit flipped: 0x00 -> 0x80
			assert_eq!(key_zero[offset], 0x80);
			// 127 with sign bit flipped: 0x7F -> 0xFF
			assert_eq!(key_pos[offset], 0xFF);

			// Verify ordering
			assert!(key_neg.as_slice() < key_zero.as_slice());
			assert!(key_zero.as_slice() < key_pos.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int1],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_zero = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_i8(&mut key_neg, 0, -128i8);
			layout.set_i8(&mut key_zero, 0, 0i8);
			layout.set_i8(&mut key_pos, 0, 127i8);

			let offset = layout.fields[0].offset;
			// -128: 0x80 -> flip sign: 0x00 -> invert: 0xFF
			assert_eq!(key_neg[offset], 0xFF);
			// 0: 0x00 -> flip sign: 0x80 -> invert: 0x7F
			assert_eq!(key_zero[offset], 0x7F);
			// 127: 0x7F -> flip sign: 0xFF -> invert: 0x00
			assert_eq!(key_pos[offset], 0x00);

			// Verify ordering (reversed)
			assert!(key_neg.as_slice() > key_zero.as_slice());
			assert!(key_zero.as_slice() > key_pos.as_slice());
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_zero = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_i32(&mut key_neg, 0, i32::MIN);
			layout.set_i32(&mut key_zero, 0, 0i32);
			layout.set_i32(&mut key_pos, 0, i32::MAX);

			let offset = layout.fields[0].offset;
			// i32::MIN in big-endian with sign bit flipped
			assert_eq!(
				&key_neg[offset..offset + 4],
				&[0x00, 0x00, 0x00, 0x00]
			);
			// 0 with sign bit flipped
			assert_eq!(
				&key_zero[offset..offset + 4],
				&[0x80, 0x00, 0x00, 0x00]
			);
			// i32::MAX with sign bit flipped
			assert_eq!(
				&key_pos[offset..offset + 4],
				&[0xFF, 0xFF, 0xFF, 0xFF]
			);

			// Verify ordering
			assert!(key_neg.as_slice() < key_zero.as_slice());
			assert!(key_zero.as_slice() < key_pos.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_zero = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_i32(&mut key_neg, 0, i32::MIN);
			layout.set_i32(&mut key_zero, 0, 0i32);
			layout.set_i32(&mut key_pos, 0, i32::MAX);

			let offset = layout.fields[0].offset;
			// i32::MIN: flip sign then invert all
			assert_eq!(
				&key_neg[offset..offset + 4],
				&[0xFF, 0xFF, 0xFF, 0xFF]
			);
			// 0: flip sign then invert all
			assert_eq!(
				&key_zero[offset..offset + 4],
				&[0x7F, 0xFF, 0xFF, 0xFF]
			);
			// i32::MAX: flip sign then invert all
			assert_eq!(
				&key_pos[offset..offset + 4],
				&[0x00, 0x00, 0x00, 0x00]
			);

			// Verify ordering (reversed)
			assert!(key_neg.as_slice() > key_zero.as_slice());
			assert!(key_zero.as_slice() > key_pos.as_slice());
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int8],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_i64(&mut key, 0, -1i64);

			let offset = layout.fields[0].offset;
			// -1 in two's complement is all 1s, with sign bit
			// flipped becomes 0x7F...
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
					0xFF, 0xFF
				]
			);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_i64(&mut key, 0, -1i64);

			let offset = layout.fields[0].offset;
			// -1: flip sign then invert all
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
					0x00, 0x00
				]
			);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint1],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key_min = layout.allocate_key();
			let mut key_mid = layout.allocate_key();
			let mut key_max = layout.allocate_key();

			layout.set_u8(&mut key_min, 0, 0u8);
			layout.set_u8(&mut key_mid, 0, 128u8);
			layout.set_u8(&mut key_max, 0, 255u8);

			let offset = layout.fields[0].offset;
			assert_eq!(key_min[offset], 0x00);
			assert_eq!(key_mid[offset], 0x80);
			assert_eq!(key_max[offset], 0xFF);

			// Verify ordering
			assert!(key_min.as_slice() < key_mid.as_slice());
			assert!(key_mid.as_slice() < key_max.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint1],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key_min = layout.allocate_key();
			let mut key_mid = layout.allocate_key();
			let mut key_max = layout.allocate_key();

			layout.set_u8(&mut key_min, 0, 0u8);
			layout.set_u8(&mut key_mid, 0, 128u8);
			layout.set_u8(&mut key_max, 0, 255u8);

			let offset = layout.fields[0].offset;
			// Inverted for DESC
			assert_eq!(key_min[offset], 0xFF);
			assert_eq!(key_mid[offset], 0x7F);
			assert_eq!(key_max[offset], 0x00);

			// Verify ordering (reversed)
			assert!(key_min.as_slice() > key_mid.as_slice());
			assert!(key_mid.as_slice() > key_max.as_slice());
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint4],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_u32(&mut key, 0, 0x12345678u32);

			let offset = layout.fields[0].offset;
			// Big-endian representation
			assert_eq!(
				&key[offset..offset + 4],
				&[0x12, 0x34, 0x56, 0x78]
			);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_u32(&mut key, 0, 0x12345678u32);

			let offset = layout.fields[0].offset;
			// Inverted for DESC
			assert_eq!(
				&key[offset..offset + 4],
				&[0xED, 0xCB, 0xA9, 0x87]
			);
		}
	}

	mod u64 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint8],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_u64(&mut key, 0, u64::MAX);

			let offset = layout.fields[0].offset;
			assert_eq!(&key[offset..offset + 8], &[0xFF; 8]);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_u64(&mut key, 0, u64::MAX);

			let offset = layout.fields[0].offset;
			assert_eq!(&key[offset..offset + 8], &[0x00; 8]);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float4],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_zero = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_f32(&mut key_neg, 0, -1.0f32);
			layout.set_f32(&mut key_zero, 0, 0.0f32);
			layout.set_f32(&mut key_pos, 0, 1.0f32);

			let offset = layout.fields[0].offset;

			// -1.0f32: 0xBF800000 -> invert all: 0x407FFFFF
			assert_eq!(
				&key_neg[offset..offset + 4],
				&[0x40, 0x7F, 0xFF, 0xFF]
			);
			// 0.0f32: 0x00000000 -> flip sign: 0x80000000
			assert_eq!(
				&key_zero[offset..offset + 4],
				&[0x80, 0x00, 0x00, 0x00]
			);
			// 1.0f32: 0x3F800000 -> flip sign: 0xBF800000
			assert_eq!(
				&key_pos[offset..offset + 4],
				&[0xBF, 0x80, 0x00, 0x00]
			);

			// Verify ordering
			assert!(key_neg.as_slice() < key_zero.as_slice());
			assert!(key_zero.as_slice() < key_pos.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key_neg = layout.allocate_key();
			let mut key_pos = layout.allocate_key();

			layout.set_f32(&mut key_neg, 0, -1.0f32);
			layout.set_f32(&mut key_pos, 0, 1.0f32);

			let offset = layout.fields[0].offset;

			// -1.0f32: ASC encoding then invert for DESC
			assert_eq!(
				&key_neg[offset..offset + 4],
				&[0xBF, 0x80, 0x00, 0x00]
			);
			// 1.0f32: ASC encoding then invert for DESC
			assert_eq!(
				&key_pos[offset..offset + 4],
				&[0x40, 0x7F, 0xFF, 0xFF]
			);

			// Verify ordering (reversed)
			assert!(key_neg.as_slice() > key_pos.as_slice());
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float8],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_f64(&mut key, 0, std::f64::consts::PI);

			let offset = layout.fields[0].offset;
			// PI in IEEE 754: 0x400921FB54442D18 -> flip sign bit
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0xC0, 0x09, 0x21, 0xFB, 0x54, 0x44,
					0x2D, 0x18
				]
			);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_f64(&mut key, 0, std::f64::consts::PI);

			let offset = layout.fields[0].offset;
			// PI: ASC encoding then invert for DESC
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0x3F, 0xF6, 0xDE, 0x04, 0xAB, 0xBB,
					0xD2, 0xE7
				]
			);
		}
	}

	mod row_number {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::RowNumber],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_row_number(&mut key, 0, 0x123456789ABCDEFu64);

			let offset = layout.fields[0].offset;
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0x01, 0x23, 0x45, 0x67, 0x89, 0xAB,
					0xCD, 0xEF
				]
			);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::RowNumber],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			layout.set_row_number(&mut key, 0, 0x123456789ABCDEFu64);

			let offset = layout.fields[0].offset;
			// Inverted for DESC
			assert_eq!(
				&key[offset..offset + 8],
				&[
					0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
					0x32, 0x10
				]
			);
		}
	}

	mod date {
		use super::*;
		use crate::value::Date;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Date],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			let date = Date::new(2025, 1, 1).unwrap();
			layout.set_date(&mut key, 0, date);

			let offset = layout.fields[0].offset;
			// Date is stored as i32 days since epoch with sign bit
			// flipped
			let bytes = &key[offset..offset + 4];

			// Verify it's properly encoded
			let mut expected =
				date.to_days_since_epoch().to_be_bytes();
			expected[0] ^= 0x80;
			assert_eq!(bytes, expected);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Date],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			let date = Date::new(2025, 1, 1).unwrap();
			layout.set_date(&mut key, 0, date);

			let offset = layout.fields[0].offset;
			let bytes = &key[offset..offset + 4];

			// Date with sign bit flipped then all inverted for DESC
			let mut expected =
				date.to_days_since_epoch().to_be_bytes();
			expected[0] ^= 0x80;
			for b in expected.iter_mut() {
				*b = !*b;
			}
			assert_eq!(bytes, expected);
		}
	}

	mod composite {
		use super::*;

		#[test]
		fn test_mixed_directions() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4, Type::Uint8],
				&[SortDirection::Desc, SortDirection::Asc],
			)
			.unwrap();

			let mut key = layout.allocate_key();
			layout.set_i32(&mut key, 0, 100);
			layout.set_u64(&mut key, 1, 200u64);

			// Check first field (i32 DESC)
			let offset1 = layout.fields[0].offset;
			let mut expected_i32 = 100i32.to_be_bytes();
			expected_i32[0] ^= 0x80;
			for b in expected_i32.iter_mut() {
				*b = !*b;
			}
			assert_eq!(&key[offset1..offset1 + 4], expected_i32);

			// Check second field (u64 ASC)
			let offset2 = layout.fields[1].offset;
			let expected_u64 = 200u64.to_be_bytes();
			assert_eq!(&key[offset2..offset2 + 8], expected_u64);
		}
	}

	mod uuid4 {
		use super::*;
		use crate::value::Uuid4;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uuid4],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			let uuid1 = Uuid4::generate();
			let uuid2 = Uuid4::generate();

			layout.set_uuid4(&mut key1, 0, uuid1.clone());
			layout.set_uuid4(&mut key2, 0, uuid2.clone());

			// Check bitvec shows field is set
			assert!(key1.is_defined(0));
			assert!(key2.is_defined(0));

			// Check values are stored correctly (16 bytes)
			let offset = layout.fields[0].offset;
			let uuid1_bytes: Vec<u8> = uuid1.as_bytes().to_vec();
			let uuid2_bytes: Vec<u8> = uuid2.as_bytes().to_vec();

			assert_eq!(
				&key1[offset..offset + 16],
				&uuid1_bytes[..]
			);
			assert_eq!(
				&key2[offset..offset + 16],
				&uuid2_bytes[..]
			);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uuid4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			let uuid = Uuid4::generate();
			layout.set_uuid4(&mut key, 0, uuid.clone());

			// Check value is inverted for DESC
			let offset = layout.fields[0].offset;
			let mut expected_bytes = uuid.as_bytes().to_vec();
			for b in expected_bytes.iter_mut() {
				*b = !*b;
			}

			assert_eq!(
				&key[offset..offset + 16],
				&expected_bytes[..]
			);
		}
	}

	mod uuid7 {
		use super::*;
		use crate::value::Uuid7;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uuid7],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			let uuid1 = Uuid7::generate();
			// Sleep a bit to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let uuid2 = Uuid7::generate();

			layout.set_uuid7(&mut key1, 0, uuid1.clone());
			layout.set_uuid7(&mut key2, 0, uuid2.clone());

			// Check bitvec shows field is set
			assert!(key1.is_defined(0));
			assert!(key2.is_defined(0));

			// Check values are stored correctly (16 bytes)
			let offset = layout.fields[0].offset;
			let uuid1_bytes: Vec<u8> = uuid1.as_bytes().to_vec();
			let uuid2_bytes: Vec<u8> = uuid2.as_bytes().to_vec();

			assert_eq!(
				&key1[offset..offset + 16],
				&uuid1_bytes[..]
			);
			assert_eq!(
				&key2[offset..offset + 16],
				&uuid2_bytes[..]
			);

			// UUID7 has timestamp prefix, so later should be
			// greater
			assert!(key1.as_slice() < key2.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uuid7],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			let uuid1 = Uuid7::generate();
			// Sleep a bit to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let uuid2 = Uuid7::generate();

			layout.set_uuid7(&mut key1, 0, uuid1.clone());
			layout.set_uuid7(&mut key2, 0, uuid2.clone());

			// Check values are inverted for DESC
			let offset = layout.fields[0].offset;
			let mut expected_bytes1 = uuid1.as_bytes().to_vec();
			let mut expected_bytes2 = uuid2.as_bytes().to_vec();
			for b in expected_bytes1.iter_mut() {
				*b = !*b;
			}
			for b in expected_bytes2.iter_mut() {
				*b = !*b;
			}

			assert_eq!(
				&key1[offset..offset + 16],
				&expected_bytes1[..]
			);
			assert_eq!(
				&key2[offset..offset + 16],
				&expected_bytes2[..]
			);

			// Verify ordering (reversed due to DESC)
			assert!(key1.as_slice() > key2.as_slice());
		}
	}

	mod identity_id {
		use super::*;
		use crate::value::IdentityId;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::IdentityId],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			let id1 = IdentityId::generate();
			// Sleep a bit to ensure different timestamps
			// (IdentityId wraps Uuid7)
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let id2 = IdentityId::generate();

			layout.set_identity_id(&mut key1, 0, id1.clone());
			layout.set_identity_id(&mut key2, 0, id2.clone());

			// Check bitvec shows field is set
			assert!(key1.is_defined(0));
			assert!(key2.is_defined(0));

			// Check values are stored correctly (16 bytes)
			let offset = layout.fields[0].offset;
			let uuid7_1: crate::value::Uuid7 = id1.into();
			let uuid7_2: crate::value::Uuid7 = id2.into();
			let id1_bytes: Vec<u8> = uuid7_1.as_bytes().to_vec();
			let id2_bytes: Vec<u8> = uuid7_2.as_bytes().to_vec();

			assert_eq!(&key1[offset..offset + 16], &id1_bytes[..]);
			assert_eq!(&key2[offset..offset + 16], &id2_bytes[..]);

			// IdentityId wraps Uuid7 which has timestamp prefix, so
			// later should be greater
			assert!(key1.as_slice() < key2.as_slice());
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::IdentityId],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			let id1 = IdentityId::generate();
			// Sleep a bit to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let id2 = IdentityId::generate();

			layout.set_identity_id(&mut key1, 0, id1.clone());
			layout.set_identity_id(&mut key2, 0, id2.clone());

			// Check values are inverted for DESC
			let offset = layout.fields[0].offset;
			let uuid7_1: crate::value::Uuid7 = id1.into();
			let uuid7_2: crate::value::Uuid7 = id2.into();
			let mut expected_bytes1 = uuid7_1.as_bytes().to_vec();
			let mut expected_bytes2 = uuid7_2.as_bytes().to_vec();
			for b in expected_bytes1.iter_mut() {
				*b = !*b;
			}
			for b in expected_bytes2.iter_mut() {
				*b = !*b;
			}

			assert_eq!(
				&key1[offset..offset + 16],
				&expected_bytes1[..]
			);
			assert_eq!(
				&key2[offset..offset + 16],
				&expected_bytes2[..]
			);

			// Verify ordering (reversed due to DESC)
			assert!(key1.as_slice() > key2.as_slice());
		}
	}

	mod undefined {
		use super::*;

		#[test]
		fn test_undefined() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key = layout.allocate_key();

			// Set a value first
			layout.set_i32(&mut key, 0, 42);
			assert!(key.is_defined(0));

			// Now set it to undefined
			layout.set_undefined(&mut key, 0);
			assert!(!key.is_defined(0));

			// Check that the data is zeroed
			let offset = layout.fields[0].offset;
			assert_eq!(&key[offset..offset + 4], &[0, 0, 0, 0]);
		}
	}
}
