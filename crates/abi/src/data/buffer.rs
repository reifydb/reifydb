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
	///
	/// # Safety
	/// Caller must ensure the pointer is valid and the buffer has not been freed.
	pub unsafe fn as_slice(&self) -> &[u8] {
		if self.is_empty() {
			&[]
		} else {
			// SAFETY: Caller must ensure pointer validity and lifetime
			unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
		}
	}
}
