// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

/// A builder for constructing binary keys using keycode encoding
///
/// This is a re-export wrapper that uses the implementation from reifydb_core
/// at the point of use, avoiding circular dependencies.
pub struct KeySerializer {
	buffer: Vec<u8>,
}

impl KeySerializer {
	/// Create new serializer with default capacity
	pub fn new() -> Self {
		Self {
			buffer: Vec::new(),
		}
	}

	/// Create with pre-allocated capacity
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			buffer: Vec::with_capacity(capacity),
		}
	}

	/// Extend with u64 value (bitwise NOT of big-endian)
	pub fn extend_u64<T: Into<u64>>(&mut self, value: T) -> &mut Self {
		let mut bytes = value.into().to_be_bytes();
		for b in bytes.iter_mut() {
			*b = !*b;
		}
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with u32 value (bitwise NOT of big-endian)
	pub fn extend_u32<T: Into<u32>>(&mut self, value: T) -> &mut Self {
		let mut bytes = value.into().to_be_bytes();
		for b in bytes.iter_mut() {
			*b = !*b;
		}
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with i64 value (flip sign bit, then NOT)
	pub fn extend_i64<T: Into<i64>>(&mut self, value: T) -> &mut Self {
		let mut bytes = value.into().to_be_bytes();
		bytes[0] ^= 1 << 7; // flip sign bit
		for b in bytes.iter_mut() {
			*b = !*b;
		}
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with i32 value (flip sign bit, then NOT)
	pub fn extend_i32<T: Into<i32>>(&mut self, value: T) -> &mut Self {
		let mut bytes = value.into().to_be_bytes();
		bytes[0] ^= 1 << 7; // flip sign bit
		for b in bytes.iter_mut() {
			*b = !*b;
		}
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with raw bytes (escape 0xff, terminate with 0xffff)
	pub fn extend_bytes<T: AsRef<[u8]>>(&mut self, bytes: T) -> &mut Self {
		for &byte in bytes.as_ref() {
			if byte == 0xff {
				self.buffer.push(0xff);
				self.buffer.push(0x00);
			} else {
				self.buffer.push(byte);
			}
		}
		self.buffer.push(0xff);
		self.buffer.push(0xff);
		self
	}

	/// Extend with string (UTF-8 bytes with keycode encoding)
	pub fn extend_str<T: AsRef<str>>(&mut self, s: T) -> &mut Self {
		self.extend_bytes(s.as_ref().as_bytes())
	}

	/// Consume serializer and return final buffer
	pub fn finish(self) -> Vec<u8> {
		self.buffer
	}

	/// Get current buffer length
	pub fn len(&self) -> usize {
		self.buffer.len()
	}

	/// Check if buffer is empty
	pub fn is_empty(&self) -> bool {
		self.buffer.is_empty()
	}
}

impl Default for KeySerializer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_u64_encoding() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(0u64);
		let result = serializer.finish();
		// 0u64 with bitwise NOT should be all 0xff
		assert_eq!(
			result,
			vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
		);
	}

	#[test]
	fn test_i64_encoding() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i64(0i64);
		let result = serializer.finish();
		// 0i64 should encode as 0x7fffffffffffffff after flip sign bit
		// and NOT
		assert_eq!(
			result,
			vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
		);
	}

	#[test]
	fn test_bytes_encoding() {
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(b"foo");
		let result = serializer.finish();
		// "foo" = 0x66 0x6f 0x6f, terminated with 0xffff
		assert_eq!(result, vec![0x66, 0x6f, 0x6f, 0xff, 0xff]);
	}

	#[test]
	fn test_bytes_with_escape() {
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(&[0x01, 0xff]);
		let result = serializer.finish();
		// 0x01, 0xff (escaped as 0xff00), terminated with 0xffff
		assert_eq!(result, vec![0x01, 0xff, 0x00, 0xff, 0xff]);
	}

	#[test]
	fn test_chaining() {
		let mut serializer = KeySerializer::new();
		serializer
			.extend_u64(1u64)
			.extend_bytes(b"test")
			.extend_i32(-1i32);
		let result = serializer.finish();

		// Check that we got some output (exact values tested above)
		assert!(!result.is_empty());
	}
}
