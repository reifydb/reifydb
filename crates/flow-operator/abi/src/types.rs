//! FFI-safe type definitions for operator-host communication

/// FFI-safe buffer representing a slice of bytes
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferFFI {
	/// Pointer to the data
	pub ptr: *const u8,
	/// Length of the data
	pub len: usize,
	/// Capacity of the allocated buffer
	pub cap: usize,
}

impl BufferFFI {
	/// Create an empty buffer
	pub const fn empty() -> Self {
		Self {
			ptr: core::ptr::null(),
			len: 0,
			cap: 0,
		}
	}

	/// Create a buffer from a slice
	pub fn from_slice(data: &[u8]) -> Self {
		Self {
			ptr: data.as_ptr(),
			len: data.len(),
			cap: data.len(),
		}
	}

	/// Check if the buffer is empty
	pub fn is_empty(&self) -> bool {
		self.len == 0 || self.ptr.is_null()
	}

	/// Get the buffer as a slice (unsafe - caller must ensure pointer validity)
	pub unsafe fn as_slice(&self) -> &[u8] {
		if self.is_empty() {
			&[]
		} else {
			// SAFETY: Caller must ensure pointer validity and lifetime
			unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
		}
	}
}

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
			fields: core::ptr::null(),
			field_count: 0,
			field_names: core::ptr::null(),
			bitvec_size: 0,
			static_section_size: 0,
			alignment: 1,
		}
	}

	/// Get a field by index
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

/// FFI-safe row representation
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RowFFI {
	/// Row number (unique identifier)
	pub number: u64,
	/// Encoded row data
	pub encoded: BufferFFI,
	/// Layout metadata for decoding
	pub layout: *const LayoutFFI,
}

/// Type of flow diff operation
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDiffType {
	/// Insert a new row
	Insert = 0,
	/// Update an existing row
	Update = 1,
	/// Remove a row
	Remove = 2,
}

/// FFI-safe flow diff
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowDiffFFI {
	/// Type of the diff
	pub diff_type: FlowDiffType,
	/// Previous row state (null for Insert)
	pub pre_row: *const RowFFI,
	/// New row state (null for Remove)
	pub post_row: *const RowFFI,
}

/// FFI-safe representation of flow change origin
///
/// Encodes both Internal and External origins:
/// - origin_type: 0 = Internal, 1 = External.Table, 2 = External.View, 3 = External.TableVirtual, 4 =
///   External.RingBuffer
/// - id: For Internal, this is the FlowNodeId. For External, this is the source ID (TableId, ViewId, etc.)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowOriginFFI {
	pub origin_type: u8,
	pub id: u64,
}

/// FFI-safe flow change containing multiple diffs
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowChangeFFI {
	/// Origin of this change
	pub origin: FlowOriginFFI,
	/// Number of diffs in the change
	pub diff_count: usize,
	/// Pointer to array of diffs
	pub diffs: *const FlowDiffFFI,
	/// Version number for this change
	pub version: u64,
}

impl FlowChangeFFI {
	/// Create an empty flow change with Internal origin 0
	pub const fn empty() -> Self {
		Self {
			origin: FlowOriginFFI {
				origin_type: 0,
				id: 0,
			},
			diff_count: 0,
			diffs: core::ptr::null(),
			version: 0,
		}
	}
}

/// FFI-safe collection of rows
#[repr(C)]
#[derive(Debug)]
pub struct RowsFFI {
	/// Number of rows
	pub count: usize,
	/// Pointer to array of row pointers (null entries mean row not found)
	pub rows: *mut *const RowFFI,
}

use core::ffi::c_void;

use crate::HostCallbacks;

/// FFI context passed to operators containing transaction, operator ID, and callbacks
/// This struct is shared between the host and operators to provide complete execution context
#[repr(C)]
pub struct FFIContext {
	/// Opaque pointer to the host's transaction data
	pub txn_ptr: *mut c_void,
	/// Operator ID for this operation
	pub operator_id: u64,
	/// Host callbacks for state and other operations
	pub callbacks: HostCallbacks,
}

/// Opaque handle to a state iterator (managed by host)
#[repr(C)]
pub struct StateIteratorFFI {
	_opaque: [u8; 0],
}

/// Opaque handle to a store iterator (managed by host)
#[repr(C)]
pub struct StoreIteratorFFI {
	_opaque: [u8; 0],
}
