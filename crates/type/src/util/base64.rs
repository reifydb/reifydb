// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! Simple base64 encoding/decoding implementation

use std::{error, fmt};
const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const BASE64_URL_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Base64 encoding engine
pub struct Engine {
	alphabet: &'static [u8],
	use_padding: bool,
}

impl Engine {
	/// Standard base64 with padding
	pub const STANDARD: Engine = Engine {
		alphabet: BASE64_CHARS,
		use_padding: true,
	};

	/// Standard base64 without padding
	pub const STANDARD_NO_PAD: Engine = Engine {
		alphabet: BASE64_CHARS,
		use_padding: false,
	};

	/// URL-safe base64 without padding
	pub const URL_SAFE_NO_PAD: Engine = Engine {
		alphabet: BASE64_URL_CHARS,
		use_padding: false,
	};

	/// Encode bytes to base64 string
	pub fn encode(&self, input: &[u8]) -> String {
		if input.is_empty() {
			return String::new();
		}

		let mut result = String::new();
		let mut i = 0;

		while i < input.len() {
			let b1 = input[i];
			let b2 = if i + 1 < input.len() {
				input[i + 1]
			} else {
				0
			};
			let b3 = if i + 2 < input.len() {
				input[i + 2]
			} else {
				0
			};

			let n = ((b1 as usize) << 16) | ((b2 as usize) << 8) | (b3 as usize);

			result.push(self.alphabet[(n >> 18) & 63] as char);
			result.push(self.alphabet[(n >> 12) & 63] as char);

			if i + 1 < input.len() {
				result.push(self.alphabet[(n >> 6) & 63] as char);
				if i + 2 < input.len() {
					result.push(self.alphabet[n & 63] as char);
				} else if self.use_padding {
					result.push('=');
				}
			} else if self.use_padding {
				result.push('=');
				result.push('=');
			}

			i += 3;
		}

		result
	}

	/// Decode base64 string to bytes
	pub fn decode(&self, input: &str) -> Result<Vec<u8>, DecodeError> {
		// URL-safe base64 should not have padding
		if !self.use_padding && input.contains('=') {
			return Err(DecodeError::UnexpectedPadding);
		}

		// Validate padding if present
		if self.use_padding && input.contains('=') {
			// Count trailing padding characters
			let padding_start = input.rfind(|c| c != '=').map(|i| i + 1).unwrap_or(0);
			let padding_count = input.len() - padding_start;

			// Valid base64 can only have 0, 1, or 2 padding
			// characters
			if padding_count > 2 {
				return Err(DecodeError::InvalidPadding);
			}

			// Check that padding only appears at the end
			if padding_start > 0 && input[..padding_start].contains('=') {
				return Err(DecodeError::InvalidPadding);
			}

			// Total length must be divisible by 4
			if input.len() % 4 != 0 {
				return Err(DecodeError::InvalidPadding);
			}

			// Validate padding count based on the last quantum
			// The last quantum (4 chars) can be:
			// - XXXX (no padding)
			// - XXX= (1 padding)
			// - XX== (2 padding)
			let non_padding_in_last_quantum = 4 - padding_count;
			if non_padding_in_last_quantum < 2 {
				return Err(DecodeError::InvalidPadding);
			}
		}

		let input = input.trim_end_matches('=');
		if input.is_empty() {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();
		let mut accumulator = 0u32;
		let mut bits_collected = 0;

		for ch in input.chars() {
			let value = self.char_to_value(ch)?;
			accumulator = (accumulator << 6) | (value as u32);
			bits_collected += 6;

			if bits_collected >= 8 {
				bits_collected -= 8;
				result.push((accumulator >> bits_collected) as u8);
				accumulator &= (1 << bits_collected) - 1;
			}
		}

		Ok(result)
	}

	fn char_to_value(&self, ch: char) -> Result<u8, DecodeError> {
		let byte = ch as u8;
		self.alphabet
			.iter()
			.position(|&b| b == byte)
			.map(|pos| pos as u8)
			.ok_or(DecodeError::InvalidCharacter(ch))
	}
}

#[derive(Debug)]
pub enum DecodeError {
	InvalidCharacter(char),
	UnexpectedPadding,
	InvalidPadding,
}

impl fmt::Display for DecodeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DecodeError::InvalidCharacter(ch) => {
				write!(f, "Invalid base64 character: '{}'", ch)
			}
			DecodeError::UnexpectedPadding => write!(f, "Unexpected padding in URL-safe base64"),
			DecodeError::InvalidPadding => {
				write!(f, "Invalid base64 padding")
			}
		}
	}
}

impl error::Error for DecodeError {}

// Convenience module to match the original API
pub mod engine {
	pub mod general_purpose {
		use crate::util::base64::Engine;

		pub const STANDARD: Engine = Engine::STANDARD;
		pub const STANDARD_NO_PAD: Engine = Engine::STANDARD_NO_PAD;
		pub const URL_SAFE_NO_PAD: Engine = Engine::URL_SAFE_NO_PAD;
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_standard() {
		assert_eq!(Engine::STANDARD.encode(b"Hello"), "SGVsbG8=");
		assert_eq!(Engine::STANDARD.encode(b"Hello, World!"), "SGVsbG8sIFdvcmxkIQ==");
		assert_eq!(Engine::STANDARD.encode(b""), "");
	}

	#[test]
	fn test_encode_no_pad() {
		assert_eq!(Engine::STANDARD_NO_PAD.encode(b"Hello"), "SGVsbG8");
		assert_eq!(Engine::STANDARD_NO_PAD.encode(b"Hello, World!"), "SGVsbG8sIFdvcmxkIQ");
	}

	#[test]
	fn test_decode_standard() {
		assert_eq!(Engine::STANDARD.decode("SGVsbG8=").unwrap(), b"Hello");
		assert_eq!(Engine::STANDARD.decode("SGVsbG8").unwrap(), b"Hello");
		assert_eq!(Engine::STANDARD.decode("").unwrap(), b"");
	}

	#[test]
	fn test_roundtrip() {
		let data = b"Hello, World! \x00\x01\x02\xFF";
		let encoded = Engine::STANDARD.encode(data);
		let decoded = Engine::STANDARD.decode(&encoded).unwrap();
		assert_eq!(decoded, data);
	}

	#[test]
	fn test_invalid_padding() {
		// Too many padding characters
		assert!(Engine::STANDARD.decode("SGVsbG8===").is_err());
		assert!(Engine::STANDARD.decode("SGVsbG8====").is_err());

		// Padding in the middle
		assert!(Engine::STANDARD.decode("SGVs=bG8=").is_err());

		// Invalid length with padding (not divisible by 4)
		assert!(Engine::STANDARD.decode("SGVsbG8=X").is_err());

		// Invalid: "SGVsbG8=" is 8 chars, needs 1 padding char, but has
		// 2
		assert!(Engine::STANDARD.decode("SGVsbG8==").is_err());

		// Valid padding should work
		assert!(Engine::STANDARD.decode("SGVsbG8=").is_ok()); // "Hello" - needs 1 padding
		assert!(Engine::STANDARD.decode("SGVsbA==").is_ok()); // "Hell" - needs 2 padding  
		assert!(Engine::STANDARD.decode("SGVs").is_ok()); // "Hel" - no padding needed
	}
}
