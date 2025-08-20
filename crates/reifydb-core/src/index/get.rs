// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use uuid::Uuid;

use crate::{
	SortDirection, Type,
	index::{EncodedIndexKey, EncodedIndexLayout},
	value::{Date, DateTime, IdentityId, Interval, Time, Uuid4, Uuid7},
};

impl EncodedIndexLayout {
	pub fn get_bool(&self, key: &EncodedIndexKey, index: usize) -> bool {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Bool);

		let byte = unsafe { *key.as_ptr().add(field.offset) };

		match field.direction {
			SortDirection::Asc => byte != 0,
			SortDirection::Desc => byte == 0,
		}
	}

	pub fn get_f32(&self, key: &EncodedIndexKey, index: usize) -> f32 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Float4);

		let mut bytes = [0u8; 4];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				4,
			);
		}

		// For DESC, undo the inversion first
		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		// Now undo the ASC encoding
		if bytes[0] & 0x80 != 0 {
			bytes[0] ^= 0x80;
		} else {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		f32::from_bits(u32::from_be_bytes(bytes))
	}

	pub fn get_f64(&self, key: &EncodedIndexKey, index: usize) -> f64 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Float8);

		let mut bytes = [0u8; 8];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				8,
			);
		}

		// For DESC, undo the inversion first
		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		// Now undo the ASC encoding
		if bytes[0] & 0x80 != 0 {
			bytes[0] ^= 0x80;
		} else {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		f64::from_bits(u64::from_be_bytes(bytes))
	}

	pub fn get_i8(&self, key: &EncodedIndexKey, index: usize) -> i8 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int1);

		let mut byte = unsafe { *key.as_ptr().add(field.offset) };

		match field.direction {
			SortDirection::Asc => {
				byte ^= 0x80;
			}
			SortDirection::Desc => {
				byte = !byte;
				byte ^= 0x80;
			}
		}

		i8::from_be_bytes([byte])
	}

	pub fn get_i16(&self, key: &EncodedIndexKey, index: usize) -> i16 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int2);

		let mut bytes = [0u8; 2];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				2,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in bytes.iter_mut() {
					*b = !*b;
				}
				bytes[0] ^= 0x80;
			}
		}

		i16::from_be_bytes(bytes)
	}

	pub fn get_i32(&self, key: &EncodedIndexKey, index: usize) -> i32 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int4);

		let mut bytes = [0u8; 4];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				4,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in bytes.iter_mut() {
					*b = !*b;
				}
				bytes[0] ^= 0x80;
			}
		}

		i32::from_be_bytes(bytes)
	}

	pub fn get_i64(&self, key: &EncodedIndexKey, index: usize) -> i64 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int8);

		let mut bytes = [0u8; 8];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				8,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in bytes.iter_mut() {
					*b = !*b;
				}
				bytes[0] ^= 0x80;
			}
		}

		i64::from_be_bytes(bytes)
	}

	pub fn get_i128(&self, key: &EncodedIndexKey, index: usize) -> i128 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Int16);

		let mut bytes = [0u8; 16];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				16,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in bytes.iter_mut() {
					*b = !*b;
				}
				bytes[0] ^= 0x80;
			}
		}

		i128::from_be_bytes(bytes)
	}

	pub fn get_u8(&self, key: &EncodedIndexKey, index: usize) -> u8 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint1);

		let byte = unsafe { *key.as_ptr().add(field.offset) };

		match field.direction {
			SortDirection::Asc => byte,
			SortDirection::Desc => !byte,
		}
	}

	pub fn get_u16(&self, key: &EncodedIndexKey, index: usize) -> u16 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint2);

		let mut bytes = [0u8; 2];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				2,
			);
		}

		match field.direction {
			SortDirection::Asc => u16::from_be_bytes(bytes),
			SortDirection::Desc => !u16::from_be_bytes(bytes),
		}
	}

	pub fn get_u32(&self, key: &EncodedIndexKey, index: usize) -> u32 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint4);

		let mut bytes = [0u8; 4];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				4,
			);
		}

		match field.direction {
			SortDirection::Asc => u32::from_be_bytes(bytes),
			SortDirection::Desc => !u32::from_be_bytes(bytes),
		}
	}

	pub fn get_u64(&self, key: &EncodedIndexKey, index: usize) -> u64 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint8);

		let mut bytes = [0u8; 8];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				8,
			);
		}

		match field.direction {
			SortDirection::Asc => u64::from_be_bytes(bytes),
			SortDirection::Desc => !u64::from_be_bytes(bytes),
		}
	}

	pub fn get_u128(&self, key: &EncodedIndexKey, index: usize) -> u128 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uint16);

		let mut bytes = [0u8; 16];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				16,
			);
		}

		match field.direction {
			SortDirection::Asc => u128::from_be_bytes(bytes),
			SortDirection::Desc => !u128::from_be_bytes(bytes),
		}
	}

	pub fn get_row_number(&self, key: &EncodedIndexKey, index: usize) -> u64 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::RowNumber);

		let mut bytes = [0u8; 8];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				8,
			);
		}

		match field.direction {
			SortDirection::Asc => u64::from_be_bytes(bytes),
			SortDirection::Desc => !u64::from_be_bytes(bytes),
		}
	}

	pub fn get_date(&self, key: &EncodedIndexKey, index: usize) -> Date {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Date);

		let mut bytes = [0u8; 4];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				4,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in bytes.iter_mut() {
					*b = !*b;
				}
				bytes[0] ^= 0x80;
			}
		}

		let days = i32::from_be_bytes(bytes);
		Date::from_days_since_epoch(days).unwrap()
	}

	pub fn get_datetime(
		&self,
		key: &EncodedIndexKey,
		index: usize,
	) -> DateTime {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::DateTime);

		let mut sec_bytes = [0u8; 8];
		let mut nano_bytes = [0u8; 4];

		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				sec_bytes.as_mut_ptr(),
				8,
			);
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset + 8),
				nano_bytes.as_mut_ptr(),
				4,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				sec_bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in sec_bytes.iter_mut() {
					*b = !*b;
				}
				sec_bytes[0] ^= 0x80;
				for b in nano_bytes.iter_mut() {
					*b = !*b;
				}
			}
		}

		let seconds = i64::from_be_bytes(sec_bytes);
		let nanos = u32::from_be_bytes(nano_bytes);
		DateTime::from_parts(seconds, nanos).unwrap()
	}

	pub fn get_time(&self, key: &EncodedIndexKey, index: usize) -> Time {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Time);

		let mut bytes = [0u8; 8];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				8,
			);
		}

		let nanos = match field.direction {
			SortDirection::Asc => u64::from_be_bytes(bytes),
			SortDirection::Desc => !u64::from_be_bytes(bytes),
		};

		Time::from_nanos_since_midnight(nanos).unwrap()
	}

	pub fn get_interval(
		&self,
		key: &EncodedIndexKey,
		index: usize,
	) -> Interval {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Interval);

		let mut months_bytes = [0u8; 4];
		let mut days_bytes = [0u8; 4];
		let mut nanos_bytes = [0u8; 8];

		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				months_bytes.as_mut_ptr(),
				4,
			);
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset + 4),
				days_bytes.as_mut_ptr(),
				4,
			);
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset + 8),
				nanos_bytes.as_mut_ptr(),
				8,
			);
		}

		match field.direction {
			SortDirection::Asc => {
				months_bytes[0] ^= 0x80;
				days_bytes[0] ^= 0x80;
				nanos_bytes[0] ^= 0x80;
			}
			SortDirection::Desc => {
				for b in months_bytes.iter_mut() {
					*b = !*b;
				}
				months_bytes[0] ^= 0x80;
				for b in days_bytes.iter_mut() {
					*b = !*b;
				}
				days_bytes[0] ^= 0x80;
				for b in nanos_bytes.iter_mut() {
					*b = !*b;
				}
				nanos_bytes[0] ^= 0x80;
			}
		}

		let months = i32::from_be_bytes(months_bytes);
		let days = i32::from_be_bytes(days_bytes);
		let nanos = i64::from_be_bytes(nanos_bytes);
		Interval::new(months, days, nanos)
	}

	pub fn get_uuid4(&self, key: &EncodedIndexKey, index: usize) -> Uuid4 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uuid4);

		let mut bytes = [0u8; 16];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				16,
			);
		}

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		let uuid = Uuid::from_bytes(bytes);
		Uuid4::from(uuid)
	}

	pub fn get_uuid7(&self, key: &EncodedIndexKey, index: usize) -> Uuid7 {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Uuid7);

		let mut bytes = [0u8; 16];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				16,
			);
		}

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		let uuid = Uuid::from_bytes(bytes);
		Uuid7::from(uuid)
	}

	pub fn get_identity_id(
		&self,
		key: &EncodedIndexKey,
		index: usize,
	) -> IdentityId {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::IdentityId);

		let mut bytes = [0u8; 16];
		unsafe {
			std::ptr::copy_nonoverlapping(
				key.as_ptr().add(field.offset),
				bytes.as_mut_ptr(),
				16,
			);
		}

		if field.direction == SortDirection::Desc {
			for b in bytes.iter_mut() {
				*b = !*b;
			}
		}

		let uuid = Uuid::from_bytes(bytes);
		let uuid7 = Uuid7::from(uuid);
		IdentityId::from(uuid7)
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		SortDirection, Type, index::EncodedIndexLayout, value::Date,
	};

	mod bool {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Bool],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			layout.set_bool(&mut key1, 0, false);
			layout.set_bool(&mut key2, 0, true);

			assert!(key1.as_slice() < key2.as_slice());
			assert_eq!(layout.get_bool(&key1, 0), false);
			assert_eq!(layout.get_bool(&key2, 0), true);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Bool],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();

			layout.set_bool(&mut key1, 0, false);
			layout.set_bool(&mut key2, 0, true);

			assert!(key1.as_slice() > key2.as_slice());
			assert_eq!(layout.get_bool(&key1, 0), false);
			assert_eq!(layout.get_bool(&key2, 0), true);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i8(&mut key1, 0, -10);
			layout.set_i8(&mut key2, 0, 0);
			layout.set_i8(&mut key3, 0, 10);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_i8(&key1, 0), -10);
			assert_eq!(layout.get_i8(&key2, 0), 0);
			assert_eq!(layout.get_i8(&key3, 0), 10);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int1],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i8(&mut key1, 0, -10);
			layout.set_i8(&mut key2, 0, 0);
			layout.set_i8(&mut key3, 0, 10);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_i8(&key1, 0), -10);
			assert_eq!(layout.get_i8(&key2, 0), 0);
			assert_eq!(layout.get_i8(&key3, 0), 10);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i32(&mut key1, 0, -1000);
			layout.set_i32(&mut key2, 0, 0);
			layout.set_i32(&mut key3, 0, 1000);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_i32(&key1, 0), -1000);
			assert_eq!(layout.get_i32(&key2, 0), 0);
			assert_eq!(layout.get_i32(&key3, 0), 1000);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i32(&mut key1, 0, -1000);
			layout.set_i32(&mut key2, 0, 0);
			layout.set_i32(&mut key3, 0, 1000);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_i32(&key1, 0), -1000);
			assert_eq!(layout.get_i32(&key2, 0), 0);
			assert_eq!(layout.get_i32(&key3, 0), 1000);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i64(&mut key1, 0, i64::MIN);
			layout.set_i64(&mut key2, 0, 0);
			layout.set_i64(&mut key3, 0, i64::MAX);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_i64(&key1, 0), i64::MIN);
			assert_eq!(layout.get_i64(&key2, 0), 0);
			assert_eq!(layout.get_i64(&key3, 0), i64::MAX);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_i64(&mut key1, 0, i64::MIN);
			layout.set_i64(&mut key2, 0, 0);
			layout.set_i64(&mut key3, 0, i64::MAX);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_i64(&key1, 0), i64::MIN);
			assert_eq!(layout.get_i64(&key2, 0), 0);
			assert_eq!(layout.get_i64(&key3, 0), i64::MAX);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u8(&mut key1, 0, 0);
			layout.set_u8(&mut key2, 0, 128);
			layout.set_u8(&mut key3, 0, 255);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_u8(&key1, 0), 0);
			assert_eq!(layout.get_u8(&key2, 0), 128);
			assert_eq!(layout.get_u8(&key3, 0), 255);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint1],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u8(&mut key1, 0, 0);
			layout.set_u8(&mut key2, 0, 128);
			layout.set_u8(&mut key3, 0, 255);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_u8(&key1, 0), 0);
			assert_eq!(layout.get_u8(&key2, 0), 128);
			assert_eq!(layout.get_u8(&key3, 0), 255);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u32(&mut key1, 0, 0u32);
			layout.set_u32(&mut key2, 0, 1000000u32);
			layout.set_u32(&mut key3, 0, u32::MAX);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_u32(&key1, 0), 0);
			assert_eq!(layout.get_u32(&key2, 0), 1000000);
			assert_eq!(layout.get_u32(&key3, 0), u32::MAX);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u32(&mut key1, 0, 0u32);
			layout.set_u32(&mut key2, 0, 1000000u32);
			layout.set_u32(&mut key3, 0, u32::MAX);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_u32(&key1, 0), 0);
			assert_eq!(layout.get_u32(&key2, 0), 1000000);
			assert_eq!(layout.get_u32(&key3, 0), u32::MAX);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u64(&mut key1, 0, 0u64);
			layout.set_u64(&mut key2, 0, 1_000_000_000u64);
			layout.set_u64(&mut key3, 0, u64::MAX);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_u64(&key1, 0), 0);
			assert_eq!(layout.get_u64(&key2, 0), 1_000_000_000);
			assert_eq!(layout.get_u64(&key3, 0), u64::MAX);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Uint8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_u64(&mut key1, 0, 0u64);
			layout.set_u64(&mut key2, 0, 1_000_000_000u64);
			layout.set_u64(&mut key3, 0, u64::MAX);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_u64(&key1, 0), 0);
			assert_eq!(layout.get_u64(&key2, 0), 1_000_000_000);
			assert_eq!(layout.get_u64(&key3, 0), u64::MAX);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_f32(&mut key1, 0, -100.5);
			layout.set_f32(&mut key2, 0, 0.0);
			layout.set_f32(&mut key3, 0, 100.5);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_f32(&key1, 0), -100.5);
			assert_eq!(layout.get_f32(&key2, 0), 0.0);
			assert_eq!(layout.get_f32(&key3, 0), 100.5);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float4],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_f32(&mut key1, 0, -100.5);
			layout.set_f32(&mut key2, 0, 0.0);
			layout.set_f32(&mut key3, 0, 100.5);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_f32(&key1, 0), -100.5);
			assert_eq!(layout.get_f32(&key2, 0), 0.0);
			assert_eq!(layout.get_f32(&key3, 0), 100.5);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_f64(&mut key1, 0, -1000.123456);
			layout.set_f64(&mut key2, 0, 0.0);
			layout.set_f64(&mut key3, 0, 1000.123456);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_f64(&key1, 0), -1000.123456);
			assert_eq!(layout.get_f64(&key2, 0), 0.0);
			assert_eq!(layout.get_f64(&key3, 0), 1000.123456);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Float8],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_f64(&mut key1, 0, -1000.123456);
			layout.set_f64(&mut key2, 0, 0.0);
			layout.set_f64(&mut key3, 0, 1000.123456);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_f64(&key1, 0), -1000.123456);
			assert_eq!(layout.get_f64(&key2, 0), 0.0);
			assert_eq!(layout.get_f64(&key3, 0), 1000.123456);
		}
	}

	mod date {
		use super::*;

		#[test]
		fn test_asc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Date],
				&[SortDirection::Asc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			let date1 = Date::new(2020, 1, 1).unwrap();
			let date2 = Date::new(2023, 6, 15).unwrap();
			let date3 = Date::new(2025, 12, 31).unwrap();

			layout.set_date(&mut key1, 0, date1.clone());
			layout.set_date(&mut key2, 0, date2.clone());
			layout.set_date(&mut key3, 0, date3.clone());

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_date(&key1, 0), date1);
			assert_eq!(layout.get_date(&key2, 0), date2);
			assert_eq!(layout.get_date(&key3, 0), date3);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::Date],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			let date1 = Date::new(2020, 1, 1).unwrap();
			let date2 = Date::new(2023, 6, 15).unwrap();
			let date3 = Date::new(2025, 12, 31).unwrap();

			layout.set_date(&mut key1, 0, date1.clone());
			layout.set_date(&mut key2, 0, date2.clone());
			layout.set_date(&mut key3, 0, date3.clone());

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_date(&key1, 0), date1);
			assert_eq!(layout.get_date(&key2, 0), date2);
			assert_eq!(layout.get_date(&key3, 0), date3);
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
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_row_number(&mut key1, 0, 1u64);
			layout.set_row_number(&mut key2, 0, 1000u64);
			layout.set_row_number(&mut key3, 0, u64::MAX);

			assert!(key1.as_slice() < key2.as_slice());
			assert!(key2.as_slice() < key3.as_slice());
			assert_eq!(layout.get_row_number(&key1, 0), 1);
			assert_eq!(layout.get_row_number(&key2, 0), 1000);
			assert_eq!(layout.get_row_number(&key3, 0), u64::MAX);
		}

		#[test]
		fn test_desc() {
			let layout = EncodedIndexLayout::new(
				&[Type::RowNumber],
				&[SortDirection::Desc],
			)
			.unwrap();
			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();

			layout.set_row_number(&mut key1, 0, 1u64);
			layout.set_row_number(&mut key2, 0, 1000u64);
			layout.set_row_number(&mut key3, 0, u64::MAX);

			assert!(key1.as_slice() > key2.as_slice());
			assert!(key2.as_slice() > key3.as_slice());
			assert_eq!(layout.get_row_number(&key1, 0), 1);
			assert_eq!(layout.get_row_number(&key2, 0), 1000);
			assert_eq!(layout.get_row_number(&key3, 0), u64::MAX);
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
			// Sleep to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let id2 = IdentityId::generate();

			layout.set_identity_id(&mut key1, 0, id1.clone());
			layout.set_identity_id(&mut key2, 0, id2.clone());

			// Should be ordered by timestamp
			assert!(key1.as_slice() < key2.as_slice());
			// Should decode back to original values
			assert_eq!(layout.get_identity_id(&key1, 0), id1);
			assert_eq!(layout.get_identity_id(&key2, 0), id2);
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
			// Sleep to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(
				10,
			));
			let id2 = IdentityId::generate();

			layout.set_identity_id(&mut key1, 0, id1.clone());
			layout.set_identity_id(&mut key2, 0, id2.clone());

			// Should be reverse ordered for DESC
			assert!(key1.as_slice() > key2.as_slice());
			// Should decode back to original values
			assert_eq!(layout.get_identity_id(&key1, 0), id1);
			assert_eq!(layout.get_identity_id(&key2, 0), id2);
		}

		#[test]
		fn test_roundtrip() {
			let layout = EncodedIndexLayout::new(
				&[Type::IdentityId],
				&[SortDirection::Asc],
			)
			.unwrap();

			let id = IdentityId::generate();
			let mut key = layout.allocate_key();

			// Set and get should preserve the value
			layout.set_identity_id(&mut key, 0, id.clone());
			let retrieved = layout.get_identity_id(&key, 0);
			assert_eq!(retrieved, id);
		}
	}

	mod composite {
		use super::*;

		#[test]
		fn test_mixed_directions() {
			let layout = EncodedIndexLayout::new(
				&[Type::Int4, Type::Uint8, Type::RowNumber],
				&[
					SortDirection::Desc,
					SortDirection::Asc,
					SortDirection::Asc,
				],
			)
			.unwrap();

			let mut key1 = layout.allocate_key();
			let mut key2 = layout.allocate_key();
			let mut key3 = layout.allocate_key();
			let mut key4 = layout.allocate_key();

			layout.set_i32(&mut key1, 0, 100);
			layout.set_u64(&mut key1, 1, 1u64);
			layout.set_row_number(&mut key1, 2, 1u64);

			layout.set_i32(&mut key2, 0, 100);
			layout.set_u64(&mut key2, 1, 2u64);
			layout.set_row_number(&mut key2, 2, 1u64);

			layout.set_i32(&mut key3, 0, 50);
			layout.set_u64(&mut key3, 1, 1u64);
			layout.set_row_number(&mut key3, 2, 1u64);

			layout.set_i32(&mut key4, 0, 50);
			layout.set_u64(&mut key4, 1, 1u64);
			layout.set_row_number(&mut key4, 2, 2u64);

			// key1 (100, 1, 1) vs key2 (100, 2, 1): same first
			// field, second field ascending
			assert!(key1.as_slice() < key2.as_slice());
			// key1 (100, 1, 1) vs key3 (50, 1, 1): first field is
			// DESC, so 100 < 50 in byte order
			assert!(key1.as_slice() < key3.as_slice());
			// key3 (50, 1, 1) vs key4 (50, 1, 2): same first two
			// fields, third field ascending
			assert!(key3.as_slice() < key4.as_slice());
		}
	}
}
