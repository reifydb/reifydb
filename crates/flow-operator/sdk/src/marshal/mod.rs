//! Marshalling implementations for FFI types

use crate::ffi::Arena;

pub(crate) mod flow;
pub(crate) mod layout;
pub(crate) mod row;

/// Marshaller for converting between Rust and FFI types
pub struct Marshaller {
	pub(crate) arena: Arena,
}

impl Marshaller {
	/// Create a new marshaller
	pub fn new() -> Self {
		Self {
			arena: Arena::new(),
		}
	}

	/// Clear the arena
	pub fn clear(&mut self) {
		self.arena.clear();
	}
}

impl Default for Marshaller {
	fn default() -> Self {
		Self::new()
	}
}
