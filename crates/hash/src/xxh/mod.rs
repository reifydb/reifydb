// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{Hash32, Hash64, Hash128};

mod binding;

#[inline(always)]
pub fn xxh32(data: &[u8], seed: u32) -> Hash32 {
	Hash32(unsafe { binding::XXH32(data.as_ptr(), data.len(), seed) })
}

#[inline(always)]
pub fn xxh64(data: &[u8], seed: u64) -> Hash64 {
	Hash64(unsafe { binding::XXH64(data.as_ptr(), data.len(), seed) })
}

#[inline(always)]
pub fn xxh3_64(data: &[u8]) -> Hash64 {
	Hash64(unsafe { binding::XXH3_64bits(data.as_ptr(), data.len()) })
}

#[inline(always)]
pub fn xxh3_128(data: &[u8]) -> Hash128 {
	let result = unsafe { binding::XXH3_128bits(data.as_ptr(), data.len()) };
	Hash128((result.high as u128) << 64 | result.low as u128)
}

#[cfg(test)]
mod tests {
	mod xxh32 {
		use crate::{xxh::Hash32, xxh32};

		#[test]
		fn test_empty_input() {
			let result = xxh32(b"", 0);
			assert_eq!(result, Hash32(0x02cc5d05));
		}

		#[test]
		fn test_simple_string() {
			let result = xxh32(b"hello", 0);
			assert_eq!(result, Hash32(4211111929));
		}

		#[test]
		fn test_with_seed() {
			let result = xxh32(b"hello", 42);
			assert_eq!(result, Hash32(1292028262));
		}

		#[test]
		fn test_longer_input() {
			let data = b"The quick brown fox jumps over the lazy dog";
			let result = xxh32(data, 0);
			assert_eq!(result, Hash32(0xe85ea4de));
		}
	}

	mod xxh64 {
		use crate::{xxh::Hash64, xxh64};

		#[test]
		fn test_empty_input() {
			let result = xxh64(b"", 0);
			assert_eq!(result, Hash64(0xef46db3751d8e999));
		}

		#[test]
		fn test_simple_string() {
			let result = xxh64(b"hello", 0);
			assert_eq!(result, Hash64(2794345569481354659));
		}

		#[test]
		fn test_with_seed() {
			let result = xxh64(b"hello", 42);
			assert_eq!(result, Hash64(14078989533569169714));
		}

		#[test]
		fn test_longer_input() {
			let data = b"The quick brown fox jumps over the lazy dog";
			let result = xxh64(data, 0);
			assert_eq!(result, Hash64(0x0b242d361fda71bc));
		}
	}

	mod xxh3_64 {
		use crate::{xxh::Hash64, xxh3_64};

		#[test]
		fn test_empty_input() {
			let result = xxh3_64(b"");
			assert_eq!(result, Hash64(0x2d06800538d394c2));
		}

		#[test]
		fn test_simple_string() {
			let result = xxh3_64(b"hello");
			assert_eq!(result, Hash64(10760762337991515389));
		}

		#[test]
		fn test_longer_input() {
			let data = b"The quick brown fox jumps over the lazy dog";
			let result = xxh3_64(data);
			assert_eq!(result, Hash64(14879076941462221669));
		}

		#[test]
		fn test_large_input() {
			let data = vec![b'a'; 1024];
			let result = xxh3_64(&data);
			assert_eq!(result, Hash64(5358556820880783900));
		}
	}

	mod xxh3_128 {
		use crate::{xxh::Hash128, xxh3_128};

		#[test]
		fn test_empty_input() {
			let result = xxh3_128(b"");
			assert_eq!(result, Hash128(0x99aa06d3014798d86001c324468d497f));
		}

		#[test]
		fn test_simple_string() {
			let result = xxh3_128(b"hello");
			assert_eq!(result, Hash128(241804000618833338782870102822322583576));
		}

		#[test]
		fn test_longer_input() {
			let data = b"The quick brown fox jumps over the lazy dog";
			let result = xxh3_128(data);
			assert_eq!(result, Hash128(294872163752933124907483712859046311505));
		}

		#[test]
		fn test_large_input() {
			let data = vec![b'a'; 1024];
			let result = xxh3_128(&data);
			assert_eq!(result, Hash128(109508391914506197641422089710284077596));
		}
	}
}
