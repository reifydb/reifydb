// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::{
	Blob, Date, DateTime, IdentityId, Interval, Time, Type, Uuid4, Uuid7,
};
use uuid::Uuid;

use crate::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_bool(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<bool>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Bool);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut bool,
				value.into(),
			)
		}
	}

	pub fn set_f32(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<f32>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Float4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut f32,
				value.into(),
			)
		}
	}

	pub fn set_f64(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<f64>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Float8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut f64,
				value.into(),
			)
		}
	}

	pub fn set_i8(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<i8>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int1);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i8,
				value.into(),
			)
		}
	}

	pub fn set_i16(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<i16>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int2);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i16,
				value.into(),
			)
		}
	}

	pub fn set_i32(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<i32>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i32,
				value.into(),
			)
		}
	}

	pub fn set_i64(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<i64>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i64,
				value.into(),
			)
		}
	}

	pub fn set_i128(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<i128>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int16);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i128,
				value.into(),
			)
		}
	}

	pub fn set_utf8(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl AsRef<str>,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Utf8);
		debug_assert!(
			!row.is_defined(index),
			"UTF8 field {} already set",
			index
		);

		let bytes = value.as_ref().as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append string to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice =
			&mut row.0.make_mut()[field.offset..field.offset + 8];
		ref_slice[0..4].copy_from_slice(
			&(dynamic_offset as u32).to_le_bytes(),
		);
		ref_slice[4..8]
			.copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn set_blob(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: &Blob,
	) {
		let field = &self.fields[index];
		debug_assert_eq!(field.value, Type::Blob);
		debug_assert!(
			!row.is_defined(index),
			"BLOB field {} already set",
			index
		);

		let bytes = value.as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append blob bytes to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice =
			&mut row.0.make_mut()[field.offset..field.offset + 8];
		ref_slice[0..4].copy_from_slice(
			&(dynamic_offset as u32).to_le_bytes(),
		);
		ref_slice[4..8]
			.copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn set_u8(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<u8>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint1);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset),
				value.into(),
			)
		}
	}

	pub fn set_u16(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<u16>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint2);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u16,
				value.into(),
			)
		}
	}

	pub fn set_u32(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<u32>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint4);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u32,
				value.into(),
			)
		}
	}

	pub fn set_u64(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<u64>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint8);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u64,
				value.into(),
			)
		}
	}

	pub fn set_u128(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: impl Into<u128>,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint16);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u128,
				value.into(),
			)
		}
	}

	pub fn set_date(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Date,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Date);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i32,
				value.to_days_since_epoch(),
			)
		}
	}

	pub fn set_datetime(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: DateTime,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::DateTime);
		row.set_valid(index, true);
		let (seconds, nanos) = value.to_parts();
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i64,
				seconds,
			);
			ptr::write_unaligned(
				row.make_mut()
					.as_mut_ptr()
					.add(field.offset + 8) as *mut u32,
				nanos,
			);
		}
	}

	pub fn set_time(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Time,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Time);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut u64,
				value.to_nanos_since_midnight(),
			)
		}
	}

	pub fn set_interval(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Interval,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Interval);
		row.set_valid(index, true);
		unsafe {
			// Store months (i32) at offset
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset)
					as *mut i32,
				value.get_months(),
			);
			// Store days (i32) at offset + 4
			ptr::write_unaligned(
				row.make_mut()
					.as_mut_ptr()
					.add(field.offset + 4) as *mut i32,
				value.get_days(),
			);
			// Store nanos (i64) at offset + 8
			ptr::write_unaligned(
				row.make_mut()
					.as_mut_ptr()
					.add(field.offset + 8) as *mut i64,
				value.get_nanos(),
			);
		}
	}

	pub fn set_uuid4(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Uuid4,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uuid4);
		row.set_valid(index, true);
		unsafe {
			// UUIDs are 16 bytes
			let uuid: Uuid = value.into();
			let bytes = uuid.as_bytes();
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				row.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_uuid7(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: Uuid7,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uuid7);
		row.set_valid(index, true);
		unsafe {
			// UUIDs are 16 bytes
			let uuid: Uuid = value.into();
			let bytes = uuid.as_bytes();
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				row.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_identity_id(
		&self,
		row: &mut EncodedRow,
		index: usize,
		value: IdentityId,
	) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::IdentityId);
		row.set_valid(index, true);
		unsafe {
			// Direct conversion from inner Uuid7 to Uuid
			let uuid: Uuid = value.0.into();
			let bytes = uuid.as_bytes();
			ptr::copy_nonoverlapping(
				bytes.as_ptr(),
				row.make_mut().as_mut_ptr().add(field.offset),
				16,
			);
		}
	}

	pub fn set_undefined(&self, row: &mut EncodedRow, index: usize) {
		debug_assert!(row.len() >= self.total_static_size());
		let field = &self.fields[index];

		row.set_valid(index, false);

		let buf = row.make_mut();
		let start = field.offset;
		let end = start + field.size;
		buf[start..end].fill(0);
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use reifydb_type::{
		Blob, Date, DateTime, IdentityId, Interval, Time, Type, Uuid4,
		Uuid7,
	};

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_bool_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Bool]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_bool(&mut row2, 0, true);

		assert!(row2.is_defined(0));

		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(raw[offset], 1u8);

		assert!(!row1.is_defined(0));
		assert_eq!(row1[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_f32_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Float4]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_f32(&mut row2, 0, 1.25f32);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<f32>()],
			&1.25f32.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_f64_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Float8]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_f64(&mut row2, 0, 3.5f64);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<f64>()],
			&3.5f64.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_i8_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int1]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_i8(&mut row2, 0, 42i8);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(&raw[offset..offset + 1], &42i8.to_le_bytes());

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_i16_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int2]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_i16(&mut row2, 0, -1234i16);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(&raw[offset..offset + 2], &(-1234i16).to_le_bytes());

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_i32_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int4]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_i32(&mut row2, 0, 56789i32);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(&raw[offset..offset + 4], &56789i32.to_le_bytes());

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_i64_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int8]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_i64(&mut row2, 0, 987654321i64);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + size_of::<i64>()],
			&987654321i64.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_i128_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int16]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_i128(
			&mut row2,
			0,
			123456789012345678901234567890i128,
		);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + size_of::<i128>()],
			&123456789012345678901234567890i128.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_str_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Utf8]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_utf8(&mut row2, 0, "reifydb");

		assert!(row2.is_defined(0));

		// Test using the get_str method which understands the new
		// format
		assert_eq!(layout.get_utf8(&row2, 0), "reifydb");

		assert!(!row1.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_u8_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uint1]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_u8(&mut row2, 0, 255u8);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<u8>()],
			&255u8.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_u16_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uint2]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_u16(&mut row2, 0, 65535u16);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<u16>()],
			&65535u16.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_u32_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uint4]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_u32(&mut row2, 0, 4294967295u32);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<u32>()],
			&4294967295u32.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_u64_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uint8]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_u64(&mut row2, 0, 18446744073709551615u64);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<u64>()],
			&18446744073709551615u64.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_u128_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uint16]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		layout.set_u128(
			&mut row2,
			0,
			340282366920938463463374607431768211455u128,
		);

		assert!(row2.is_defined(0));
		let raw = &row2.0;
		let offset = layout.fields[0].offset;
		assert_eq!(
			&raw[offset..offset + std::mem::size_of::<u128>()],
			&340282366920938463463374607431768211455u128
				.to_le_bytes()
		);

		assert!(!row1.is_defined(0));
		assert_eq!(row1.0[offset], 0u8);
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_set_undefined_and_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Int4]);
		let mut row1 = layout.allocate_row();

		layout.set_i32(&mut row1, 0, 12345);

		let mut row2 = row1.clone();
		assert!(row2.is_defined(0));

		layout.set_undefined(&mut row2, 0);
		assert!(!row2.is_defined(0));
		assert_eq!(layout.get_i32(&row2, 0), 0);

		assert!(row1.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(layout.get_i32(&row1, 0), 12345);
	}

	#[test]
	fn test_utf8_setting_order_variations() {
		// Test forward order
		let layout = EncodedRowLayout::new(&[
			Type::Utf8,
			Type::Utf8,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		layout.set_utf8(&mut row, 0, "first");
		layout.set_utf8(&mut row, 1, "second");
		layout.set_utf8(&mut row, 2, "third");

		assert_eq!(layout.get_utf8(&row, 0), "first");
		assert_eq!(layout.get_utf8(&row, 1), "second");
		assert_eq!(layout.get_utf8(&row, 2), "third");

		// Test reverse order
		let mut row2 = layout.allocate_row();
		layout.set_utf8(&mut row2, 2, "third");
		layout.set_utf8(&mut row2, 1, "second");
		layout.set_utf8(&mut row2, 0, "first");

		assert_eq!(layout.get_utf8(&row2, 0), "first");
		assert_eq!(layout.get_utf8(&row2, 1), "second");
		assert_eq!(layout.get_utf8(&row2, 2), "third");

		// Test random order
		let mut row3 = layout.allocate_row();
		layout.set_utf8(&mut row3, 1, "second");
		layout.set_utf8(&mut row3, 0, "first");
		layout.set_utf8(&mut row3, 2, "third");

		assert_eq!(layout.get_utf8(&row3, 0), "first");
		assert_eq!(layout.get_utf8(&row3, 1), "second");
		assert_eq!(layout.get_utf8(&row3, 2), "third");
	}

	#[test]
	fn test_utf8_with_clone_on_write_dynamic() {
		let layout = EncodedRowLayout::new(&[
			Type::Utf8,
			Type::Int4,
			Type::Utf8,
		]);
		let mut row1 = layout.allocate_row();

		layout.set_utf8(&mut row1, 0, "original_string");
		layout.set_i32(&mut row1, 1, 42);

		let mut row2 = row1.clone();
		assert_eq!(layout.get_utf8(&row2, 0), "original_string");
		assert_eq!(layout.get_i32(&row2, 1), 42);

		// Setting UTF8 on cloned row should trigger COW
		layout.set_utf8(&mut row2, 2, "new_string");

		assert_ne!(row1.as_ptr(), row2.as_ptr());
		assert_eq!(layout.get_utf8(&row1, 0), "original_string");
		assert_eq!(layout.get_utf8(&row2, 0), "original_string");
		assert_eq!(layout.get_utf8(&row2, 2), "new_string");

		// row1 should not have the second string set
		assert!(!row1.is_defined(2));
		assert!(row2.is_defined(2));
	}

	#[test]
	fn test_large_utf8_string_allocation() {
		let layout = EncodedRowLayout::new(&[
			Type::Bool,
			Type::Utf8,
			Type::Uint4,
		]);
		let mut row = layout.allocate_row();

		let initial_size = row.len();
		let large_string = "X".repeat(5000);

		layout.set_bool(&mut row, 0, false);
		layout.set_utf8(&mut row, 1, &large_string);
		layout.set_u32(&mut row, 2, 999u32);

		assert_eq!(row.len(), initial_size + 5000);
		assert_eq!(layout.dynamic_section_size(&row), 5000);
		assert_eq!(layout.get_bool(&row, 0), false);
		assert_eq!(layout.get_utf8(&row, 1), large_string);
		assert_eq!(layout.get_u32(&row, 2), 999);
	}

	#[test]
	fn test_mixed_field_types_arbitrary_order() {
		let layout = EncodedRowLayout::new(&[
			Type::Float8,
			Type::Utf8,
			Type::Bool,
			Type::Utf8,
			Type::Int2,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		// Set in completely arbitrary order
		layout.set_utf8(&mut row, 3, "middle");
		layout.set_bool(&mut row, 2, true);
		layout.set_utf8(&mut row, 5, "end");
		layout.set_f64(&mut row, 0, 3.14159);
		layout.set_i16(&mut row, 4, -500i16);
		layout.set_utf8(&mut row, 1, "beginning");

		assert_eq!(layout.get_f64(&row, 0), 3.14159);
		assert_eq!(layout.get_utf8(&row, 1), "beginning");
		assert_eq!(layout.get_bool(&row, 2), true);
		assert_eq!(layout.get_utf8(&row, 3), "middle");
		assert_eq!(layout.get_i16(&row, 4), -500);
		assert_eq!(layout.get_utf8(&row, 5), "end");
	}

	#[test]
	fn test_sparse_utf8_field_setting() {
		let layout = EncodedRowLayout::new(&[
			Type::Utf8,
			Type::Utf8,
			Type::Utf8,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		// Only set some UTF8 fields, leave others undefined
		layout.set_utf8(&mut row, 0, "first");
		layout.set_utf8(&mut row, 2, "third");
		// Skip fields 1 and 3

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));
		assert!(!row.is_defined(3));

		assert_eq!(layout.get_utf8(&row, 0), "first");
		assert_eq!(layout.get_utf8(&row, 2), "third");
	}

	#[test]
	fn test_empty_utf8_strings() {
		let layout = EncodedRowLayout::new(&[
			Type::Utf8,
			Type::Utf8,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		layout.set_utf8(&mut row, 0, "");
		layout.set_utf8(&mut row, 1, "");
		layout.set_utf8(&mut row, 2, "");

		assert_eq!(layout.get_utf8(&row, 0), "");
		assert_eq!(layout.get_utf8(&row, 1), "");
		assert_eq!(layout.get_utf8(&row, 2), "");

		// Dynamic section should exist but be empty
		assert_eq!(layout.dynamic_section_size(&row), 0);
	}

	#[test]
	fn test_utf8_memory_layout_verification() {
		let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate_row();

		let initial_size = layout.total_static_size();
		assert_eq!(row.len(), initial_size);

		layout.set_utf8(&mut row, 0, "hello"); // 5 bytes
		assert_eq!(row.len(), initial_size + 5);
		assert_eq!(layout.dynamic_section_size(&row), 5);

		layout.set_utf8(&mut row, 1, "world"); // 5 more bytes
		assert_eq!(row.len(), initial_size + 10);
		assert_eq!(layout.dynamic_section_size(&row), 10);

		assert_eq!(layout.get_utf8(&row, 0), "hello");
		assert_eq!(layout.get_utf8(&row, 1), "world");
	}

	#[test]
	fn test_utf8_with_various_unicode() {
		let layout = EncodedRowLayout::new(&[
			Type::Utf8,
			Type::Utf8,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		layout.set_utf8(&mut row, 0, "🎉"); // Emoji
		layout.set_utf8(&mut row, 1, "Ω"); // Greek
		layout.set_utf8(&mut row, 2, "日本語"); // Japanese

		assert_eq!(layout.get_utf8(&row, 0), "🎉");
		assert_eq!(layout.get_utf8(&row, 1), "Ω");
		assert_eq!(layout.get_utf8(&row, 2), "日本語");
	}

	#[test]
	fn test_date_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::Date]);
		let mut row = layout.allocate_row();

		let date = Date::new(2025, 7, 15).unwrap();
		layout.set_date(&mut row, 0, date.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_date(&row, 0), date);
	}

	#[test]
	fn test_datetime_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::DateTime]);
		let mut row = layout.allocate_row();

		let datetime =
			DateTime::new(2025, 7, 15, 14, 30, 45, 123456789)
				.unwrap();
		layout.set_datetime(&mut row, 0, datetime.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_datetime(&row, 0), datetime);
	}

	#[test]
	fn test_time_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::Time]);
		let mut row = layout.allocate_row();

		let time = Time::new(12, 34, 56, 123456789).unwrap();
		layout.set_time(&mut row, 0, time.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_time(&row, 0), time);
	}

	#[test]
	fn test_interval_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::Interval]);
		let mut row = layout.allocate_row();

		let interval = Interval::from_seconds(-3600); // -1 hour
		layout.set_interval(&mut row, 0, interval.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_interval(&row, 0), interval);
	}

	#[test]
	fn test_temporal_types_mixed_with_others() {
		let layout = EncodedRowLayout::new(&[
			Type::Date,
			Type::Bool,
			Type::DateTime,
			Type::Utf8,
			Type::Time,
			Type::Int4,
			Type::Interval,
		]);
		let mut row = layout.allocate_row();

		let date = Date::new(2022, 1, 1).unwrap();
		let datetime = DateTime::new(2022, 1, 1, 0, 0, 0, 0).unwrap();
		let time = Time::new(12, 0, 0, 0).unwrap();
		let interval = Interval::from_seconds(86400); // 1 day

		layout.set_date(&mut row, 0, date.clone());
		layout.set_bool(&mut row, 1, true);
		layout.set_datetime(&mut row, 2, datetime.clone());
		layout.set_utf8(&mut row, 3, "temporal test");
		layout.set_time(&mut row, 4, time.clone());
		layout.set_i32(&mut row, 5, 42);
		layout.set_interval(&mut row, 6, interval.clone());

		assert_eq!(layout.get_date(&row, 0), date);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_datetime(&row, 2), datetime);
		assert_eq!(layout.get_utf8(&row, 3), "temporal test");
		assert_eq!(layout.get_time(&row, 4), time);
		assert_eq!(layout.get_i32(&row, 5), 42);
		assert_eq!(layout.get_interval(&row, 6), interval);
	}

	#[test]
	fn test_uuid4_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::Uuid4]);
		let mut row = layout.allocate_row();

		let uuid = Uuid4::generate();
		layout.set_uuid4(&mut row, 0, uuid.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_uuid4(&row, 0), uuid);
	}

	#[test]
	fn test_uuid7_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::Uuid7]);
		let mut row = layout.allocate_row();

		let uuid = Uuid7::generate();
		layout.set_uuid7(&mut row, 0, uuid.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_uuid7(&row, 0), uuid);
	}

	#[test]
	fn test_identity_id_set_and_get() {
		let layout = EncodedRowLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate_row();

		let id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, id.clone());

		assert!(row.is_defined(0));
		assert_eq!(layout.get_identity_id(&row, 0), id);
	}

	#[test]
	fn test_uuid_clone_on_write() {
		let layout = EncodedRowLayout::new(&[Type::Uuid4]);
		let row1 = layout.allocate_row();
		let mut row2 = row1.clone();

		assert!(!row1.is_defined(0));
		assert!(!row2.is_defined(0));

		let uuid = Uuid4::generate();
		layout.set_uuid4(&mut row2, 0, uuid.clone());

		assert!(row2.is_defined(0));
		assert_eq!(layout.get_uuid4(&row2, 0), uuid);

		assert!(!row1.is_defined(0));
		assert_ne!(row1.as_ptr(), row2.as_ptr());
	}

	#[test]
	fn test_blob_basic() {
		let layout = EncodedRowLayout::new(&[Type::Blob]);
		let mut row = layout.allocate_row();

		let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
		let blob = Blob::new(data.clone());
		layout.set_blob(&mut row, 0, &blob);

		let retrieved_blob = layout.get_blob(&row, 0);
		assert_eq!(retrieved_blob.as_bytes(), &data);
		assert!(row.is_defined(0));
	}

	#[test]
	fn test_blob_empty() {
		let layout = EncodedRowLayout::new(&[Type::Blob]);
		let mut row = layout.allocate_row();

		let blob = Blob::new(vec![]);
		layout.set_blob(&mut row, 0, &blob);

		let retrieved_blob = layout.get_blob(&row, 0);
		assert!(retrieved_blob.is_empty());
		assert_eq!(retrieved_blob.len(), 0);
		assert!(row.is_defined(0));
	}

	#[test]
	fn test_blob_multiple() {
		let layout = EncodedRowLayout::new(&[
			Type::Blob,
			Type::Blob,
			Type::Blob,
		]);
		let mut row = layout.allocate_row();

		let blob1 = Blob::new(vec![0x00, 0x01]);
		let blob2 = Blob::new(vec![0xFF, 0xFE, 0xFD]);
		let blob3 = Blob::new(vec![0xCA, 0xFE, 0xBA, 0xBE]);

		layout.set_blob(&mut row, 0, &blob1);
		layout.set_blob(&mut row, 1, &blob2);
		layout.set_blob(&mut row, 2, &blob3);

		assert_eq!(layout.get_blob(&row, 0).as_bytes(), &[0x00, 0x01]);
		assert_eq!(
			layout.get_blob(&row, 1).as_bytes(),
			&[0xFF, 0xFE, 0xFD]
		);
		assert_eq!(
			layout.get_blob(&row, 2).as_bytes(),
			&[0xCA, 0xFE, 0xBA, 0xBE]
		);
	}

	#[test]
	fn test_blob_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[
			Type::Int4,
			Type::Blob,
			Type::Utf8,
		]);
		let mut row = layout.allocate_row();

		layout.set_i32(&mut row, 0, 42);
		let blob = Blob::new(vec![0x12, 0x34, 0x56, 0x78]);
		layout.set_blob(&mut row, 1, &blob);
		layout.set_utf8(&mut row, 2, "hello");

		assert_eq!(layout.get_i32(&row, 0), 42);
		assert_eq!(
			layout.get_blob(&row, 1).as_bytes(),
			&[0x12, 0x34, 0x56, 0x78]
		);
		assert_eq!(layout.get_utf8(&row, 2), "hello");
	}
}
