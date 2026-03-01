// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! Simple base58 encoding/decoding implementation

use std::{error, fmt};
const BASE58_CHARS: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Encode bytes to base58 string
pub fn encode(input: &[u8]) -> String {
	if input.is_empty() {
		return String::new();
	}

	// Count leading zeros
	let leading_zeros = input.iter().take_while(|&&b| b == 0).count();

	// Convert to base58 using big-integer arithmetic
	// We work with a mutable copy of the input as a big-endian number
	let mut bytes = input.to_vec();
	let mut result = Vec::new();

	while !bytes.iter().all(|&b| b == 0) {
		let mut remainder = 0u32;
		for byte in bytes.iter_mut() {
			let value = (remainder << 8) | (*byte as u32);
			*byte = (value / 58) as u8;
			remainder = value % 58;
		}
		result.push(BASE58_CHARS[remainder as usize]);
	}

	// Add leading '1's for each leading zero byte
	for _ in 0..leading_zeros {
		result.push(b'1');
	}

	// Reverse and convert to string
	result.reverse();
	String::from_utf8(result).unwrap()
}

/// Decode base58 string to bytes
pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
	if input.is_empty() {
		return Ok(Vec::new());
	}

	// Count leading '1's (they represent leading zero bytes)
	let leading_ones = input.chars().take_while(|&c| c == '1').count();

	// Convert from base58 to bytes
	let mut bytes: Vec<u8> = Vec::new();

	for ch in input.chars() {
		let value = char_to_value(ch)?;

		// Multiply existing bytes by 58 and add the new value
		let mut carry = value as u32;
		for byte in bytes.iter_mut().rev() {
			let val = (*byte as u32) * 58 + carry;
			*byte = (val & 0xFF) as u8;
			carry = val >> 8;
		}

		while carry > 0 {
			bytes.insert(0, (carry & 0xFF) as u8);
			carry >>= 8;
		}
	}

	// Prepend leading zero bytes
	let mut result = vec![0u8; leading_ones];
	result.extend(bytes);

	Ok(result)
}

fn char_to_value(ch: char) -> Result<u8, DecodeError> {
	let byte = ch as u8;
	BASE58_CHARS.iter().position(|&b| b == byte).map(|pos| pos as u8).ok_or(DecodeError::InvalidCharacter(ch))
}

#[derive(Debug)]
pub enum DecodeError {
	InvalidCharacter(char),
}

impl fmt::Display for DecodeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DecodeError::InvalidCharacter(ch) => {
				write!(f, "Invalid base58 character: '{}'", ch)
			}
		}
	}
}

impl error::Error for DecodeError {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_encode_empty() {
		assert_eq!(encode(b""), "");
	}

	#[test]
	fn test_encode_hello() {
		// "Hello" -> "9Ajdvzr"
		assert_eq!(encode(b"Hello"), "9Ajdvzr");
	}

	#[test]
	fn test_encode_hello_world() {
		// "Hello, World!" -> "72k1xXWG59fYdzSNoA"
		assert_eq!(encode(b"Hello, World!"), "72k1xXWG59fYdzSNoA");
	}

	#[test]
	fn test_encode_leading_zeros() {
		// Leading zero bytes become leading '1's
		assert_eq!(encode(&[0, 0, 1]), "112");
		assert_eq!(encode(&[0, 0, 0]), "111");
	}

	#[test]
	fn test_decode_empty() {
		assert_eq!(decode("").unwrap(), b"");
	}

	#[test]
	fn test_decode_hello() {
		assert_eq!(decode("9Ajdvzr").unwrap(), b"Hello");
	}

	#[test]
	fn test_decode_hello_world() {
		assert_eq!(decode("72k1xXWG59fYdzSNoA").unwrap(), b"Hello, World!");
	}

	#[test]
	fn test_decode_leading_ones() {
		assert_eq!(decode("112").unwrap(), &[0, 0, 1]);
		assert_eq!(decode("111").unwrap(), &[0, 0, 0]);
	}

	#[test]
	fn test_roundtrip() {
		let data = b"Hello, World! \x00\x01\x02\xFF";
		let encoded = encode(data);
		let decoded = decode(&encoded).unwrap();
		assert_eq!(decoded, data);
	}

	#[test]
	fn test_roundtrip_binary() {
		let data = &[0xde, 0xad, 0xbe, 0xef];
		let encoded = encode(data);
		let decoded = decode(&encoded).unwrap();
		assert_eq!(decoded, data);
	}

	#[test]
	fn test_invalid_character() {
		// '0', 'O', 'I', 'l' are not in base58 alphabet
		assert!(decode("0").is_err());
		assert!(decode("O").is_err());
		assert!(decode("I").is_err());
		assert!(decode("l").is_err());
		assert!(decode("invalid!").is_err());
	}
}
