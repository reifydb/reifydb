use core::ptr::null;

use super::BufferFFI;

/// FFI-safe field metadata for layout
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FieldFFI {
	/// Byte offset in encoded data
	pub offset: usize,
	/// Size in bytes
	pub size: usize,
	/// Alignment requirement
	pub align: usize,
	/// Type as u8 (use reifydb_type::Type::to_u8/from_u8)
	pub field_type: u8,
}

/// FFI-safe layout metadata for encoded rows
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LayoutFFI {
	/// Pointer to array of field metadata
	pub fields: *const FieldFFI,
	/// Number of fields
	pub field_count: usize,
	/// Pointer to array of field names (UTF-8 encoded as BufferFFI)
	pub field_names: *const BufferFFI,
	/// Size of bitvec section in bytes
	pub bitvec_size: usize,
	/// Size of static data section in bytes
	pub static_section_size: usize,
	/// Overall alignment requirement
	pub alignment: usize,
}

impl LayoutFFI {
	/// Create an empty layout
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

	/// Get a field by index
	///
	/// # Safety
	/// Caller must ensure the fields pointer is valid and the index is within bounds.
	pub unsafe fn get_field(&self, index: usize) -> Option<&FieldFFI> {
		if index < self.field_count && !self.fields.is_null() {
			// SAFETY: Caller must ensure fields pointer is valid and index is in bounds
			unsafe { Some(&*self.fields.add(index)) }
		} else {
			None
		}
	}

	/// Check if a field is defined in the encoded data
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

	/// Get the offset where data starts (after bitvec)
	pub const fn data_offset(&self) -> usize {
		self.bitvec_size
	}

	/// Get the total size of the static section (bitvec + static data)
	pub const fn total_static_size(&self) -> usize {
		self.bitvec_size + self.static_section_size
	}
}
