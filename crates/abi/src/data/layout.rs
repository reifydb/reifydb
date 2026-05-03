// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ptr::null;

use super::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FieldFFI {
	pub offset: usize,

	pub size: usize,

	pub align: usize,

	pub field_type: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LayoutFFI {
	pub fields: *const FieldFFI,

	pub field_count: usize,

	pub field_names: *const BufferFFI,

	pub bitvec_size: usize,

	pub static_section_size: usize,

	pub alignment: usize,
}

impl LayoutFFI {
	pub const fn empty() -> Self {
		Self {
			fields: null(),
			field_count: 0,
			field_names: null(),
			bitvec_size: 0,
			static_section_size: 0,
			alignment: 1,
		}
	}

	// SAFETY: caller must ensure `self.fields` points to a valid array of at least

	#[allow(clippy::missing_safety_doc)]
	pub unsafe fn get_field(&self, index: usize) -> Option<&FieldFFI> {
		if index < self.field_count && !self.fields.is_null() {
			// SAFETY: Caller must ensure fields pointer is valid and index is in bounds
			unsafe { Some(&*self.fields.add(index)) }
		} else {
			None
		}
	}

	pub fn is_defined(&self, encoded: &BufferFFI, index: usize) -> bool {
		if index >= self.field_count || encoded.is_empty() {
			return false;
		}

		let byte_index = index / 8;
		let bit_index = index % 8;

		if byte_index >= self.bitvec_size {
			return false;
		}

		let bitvec_ptr = encoded.ptr;
		let byte = unsafe { *bitvec_ptr.add(byte_index) };
		(byte & (1 << bit_index)) != 0
	}

	pub const fn data_offset(&self) -> usize {
		self.bitvec_size
	}

	pub const fn total_static_size(&self) -> usize {
		self.bitvec_size + self.static_section_size
	}
}
