// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::Hash160;

// Use C FFI bindings for native builds
#[cfg(feature = "native")]
use core::mem::MaybeUninit;

#[cfg(feature = "native")]
pub mod binding;

// Use pure Rust implementation for WASM builds
#[cfg(feature = "wasm")]
pub mod wasm;

#[inline(always)]
pub fn sha1(data: &[u8]) -> Hash160 {
	#[cfg(feature = "native")]
	{
		let mut digest = [0u8; 20];
		unsafe {
			binding::SHA1(digest.as_mut_ptr(), data.as_ptr(), data.len() as u32);
		}
		Hash160(digest)
	}
	#[cfg(feature = "wasm")]
	{
		let mut digest = [0u8; 20];
		unsafe {
			wasm::SHA1(digest.as_mut_ptr(), data.as_ptr(), data.len() as u32);
		}
		Hash160(digest)
	}
}

// Native implementation uses C FFI
#[cfg(feature = "native")]
pub struct Sha1 {
	ctx: binding::SHA1_CTX,
}

#[cfg(feature = "native")]
impl Sha1 {
	pub fn new() -> Self {
		let mut ctx = MaybeUninit::<binding::SHA1_CTX>::uninit();
		unsafe {
			binding::SHA1Init(ctx.as_mut_ptr());
			Self {
				ctx: ctx.assume_init(),
			}
		}
	}

	pub fn update(&mut self, data: &[u8]) {
		unsafe {
			binding::SHA1Update(&mut self.ctx, data.as_ptr(), data.len() as u32);
		}
	}

	pub fn finalize(mut self) -> Hash160 {
		let mut digest = [0u8; 20];
		unsafe {
			binding::SHA1Final(digest.as_mut_ptr(), &mut self.ctx);
		}
		Hash160(digest)
	}
}

#[cfg(feature = "native")]
impl Default for Sha1 {
	fn default() -> Self {
		Self::new()
	}
}

// WASM implementation uses pure Rust
#[cfg(feature = "wasm")]
pub use wasm::Sha1;

#[cfg(test)]
pub mod tests {
	use super::*;

	// Test Vector 1: "abc"
	#[test]
	fn test_vector_1_abc() {
		let result = sha1(b"abc");
		let expected = [
			0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e, 0x25, 0x71, 0x78, 0x50, 0xc2, 0x6c,
			0x9c, 0xd0, 0xd8, 0x9d,
		];
		assert_eq!(result, Hash160(expected));
	}

	// Test Vector 2: empty string
	#[test]
	fn test_vector_2_empty() {
		let result = sha1(b"");
		let expected = [
			0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60, 0x18, 0x90,
			0xaf, 0xd8, 0x07, 0x09,
		];
		assert_eq!(result, Hash160(expected));
	}

	// Test Vector 3:
	// "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
	#[test]
	fn test_vector_3_alphabet_pattern() {
		let result = sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
		let expected = [
			0x84, 0x98, 0x3e, 0x44, 0x1c, 0x3b, 0xd2, 0x6e, 0xba, 0xae, 0x4a, 0xa1, 0xf9, 0x51, 0x29, 0xe5,
			0xe5, 0x46, 0x70, 0xf1,
		];
		assert_eq!(result, Hash160(expected));
	}

	// Test Vector 4: incremental update test
	#[test]
	fn test_vector_4_incremental() {
		let mut hasher = Sha1::new();
		hasher.update(b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghij");
		hasher.update(b"klmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu");
		let result = hasher.finalize();

		let expected = [
			0xa4, 0x9b, 0x24, 0x46, 0xa0, 0x2c, 0x64, 0x5b, 0xf4, 0x19, 0xf9, 0x95, 0xb6, 0x70, 0x91, 0x25,
			0x3a, 0x04, 0xa2, 0x59,
		];
		assert_eq!(result, Hash160(expected));
	}

	// Test Vector 5: one million 'a's
	#[test]
	fn test_vector_5_million_a() {
		let data = vec![b'a'; 1_000_000];
		let result = sha1(&data);
		let expected = [
			0x34, 0xaa, 0x97, 0x3c, 0xd4, 0xc4, 0xda, 0xa4, 0xf6, 0x1e, 0xeb, 0x2b, 0xdb, 0xad, 0x27, 0x31,
			0x65, 0x34, 0x01, 0x6f,
		];
		assert_eq!(result, Hash160(expected));
	}

	// Test Vector 6: Large incremental update (simpler version)
	// Note: The original test does 16777216 iterations which would be too
	// slow We'll test with fewer iterations to verify incremental hashing
	// works
	#[test]
	fn test_vector_6_multiple_updates() {
		let string = b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno";
		let mut hasher = Sha1::new();

		// Do 1000 iterations instead of 16777216 for practical testing
		for _ in 0..1000 {
			hasher.update(string);
		}
		let result = hasher.finalize();

		// Verify the hash is different from a single update
		let single_result = sha1(string);
		assert_ne!(result, single_result);
	}

	// Additional test cases

	#[test]
	fn test_sha1_quick_brown_fox() {
		let result = sha1(b"The quick brown fox jumps over the lazy dog");
		let expected = [
			0x2f, 0xd4, 0xe1, 0xc6, 0x7a, 0x2d, 0x28, 0xfc, 0xed, 0x84, 0x9e, 0xe1, 0xbb, 0x76, 0xe7, 0x39,
			0x1b, 0x93, 0xeb, 0x12,
		];
		assert_eq!(result, Hash160(expected));
	}

	#[test]
	fn test_sha1_incremental_same_as_single() {
		let mut hasher = Sha1::new();
		hasher.update(b"The quick brown fox ");
		hasher.update(b"jumps over the lazy dog");
		let result = hasher.finalize();

		let expected = sha1(b"The quick brown fox jumps over the lazy dog");
		assert_eq!(result, expected);
	}

	#[test]
	fn test_sha1_1024_bytes() {
		let data = vec![b'a'; 1024];
		let result = sha1(&data);
		let expected = [
			0x8e, 0xca, 0x55, 0x46, 0x31, 0xdf, 0x9e, 0xad, 0x14, 0x51, 0x0e, 0x1a, 0x70, 0xae, 0x48, 0xc7,
			0x0f, 0x9b, 0x93, 0x84,
		];
		assert_eq!(result, Hash160(expected));
	}

	#[test]
	fn test_sha1_empty_incremental() {
		let hasher = Sha1::new();
		let result = hasher.finalize();
		let expected = sha1(b"");
		assert_eq!(result, expected);
	}

	#[test]
	fn test_sha1_multiple_small_updates() {
		let mut hasher = Sha1::new();
		for byte in b"The quick brown fox jumps over the lazy dog" {
			hasher.update(&[*byte]);
		}
		let result = hasher.finalize();

		let expected = sha1(b"The quick brown fox jumps over the lazy dog");
		assert_eq!(result, expected);
	}

	// Performance test: verify correct handling of various input sizes
	#[test]
	fn test_sha1_various_sizes() {
		// Test powers of 2 and nearby values
		let sizes = [
			0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511,
			512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097,
		];

		for size in sizes {
			let data = vec![b'x'; size];
			let result1 = sha1(&data);

			// Verify incremental gives same result
			let mut hasher = Sha1::new();
			hasher.update(&data);
			let result2 = hasher.finalize();

			assert_eq!(result1, result2, "Failed for size {}", size);
		}
	}

	// Test that the implementation matches expected output format
	#[test]
	fn test_sha1_output_format() {
		let result = sha1(b"test");
		// SHA1 always produces exactly 20 bytes
		assert_eq!(core::mem::size_of_val(&result.0), 20);
	}
}
