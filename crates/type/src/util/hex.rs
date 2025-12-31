// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Simple hex encoding/decoding implementation

/// Encode bytes to hex string
pub fn encode(data: &[u8]) -> String {
	let mut result = String::with_capacity(data.len() * 2);
	for byte in data {
		result.push_str(&format!("{:02x}", byte));
	}
	result
}

/// Decode hex string to bytes
pub fn decode(hex: &str) -> Result<Vec<u8>, DecodeError> {
	let hex = hex.trim();

	if hex.is_empty() {
		return Ok(Vec::new());
	}

	if hex.len() % 2 != 0 {
		return Err(DecodeError::OddLength);
	}

	let mut result = Vec::with_capacity(hex.len() / 2);

	for chunk in hex.as_bytes().chunks(2) {
		let high = decode_hex_digit(chunk[0])?;
		let low = decode_hex_digit(chunk[1])?;
		result.push((high << 4) | low);
	}

	Ok(result)
}

fn decode_hex_digit(byte: u8) -> Result<u8, DecodeError> {
	match byte {
		b'0'..=b'9' => Ok(byte - b'0'),
		b'a'..=b'f' => Ok(byte - b'a' + 10),
		b'A'..=b'F' => Ok(byte - b'A' + 10),
		_ => Err(DecodeError::InvalidCharacter(byte as char)),
	}
}

#[derive(Debug)]
pub enum DecodeError {
	InvalidCharacter(char),
	OddLength,
}

impl std::fmt::Display for DecodeError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			DecodeError::InvalidCharacter(ch) => {
				write!(f, "Invalid hex character: '{}'", ch)
			}
			DecodeError::OddLength => {
				write!(f, "Hex string has odd length")
			}
		}
	}
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_encode() {
		assert_eq!(encode(b"Hello"), "48656c6c6f");
		assert_eq!(encode(b""), "");
		assert_eq!(encode(&[0x00, 0xFF, 0x42]), "00ff42");
	}

	#[test]
	fn test_decode() {
		assert_eq!(decode("48656c6c6f").unwrap(), b"Hello");
		assert_eq!(decode("48656C6C6F").unwrap(), b"Hello");
		assert_eq!(decode("").unwrap(), b"");
		assert_eq!(decode("00ff42").unwrap(), vec![0x00, 0xFF, 0x42]);
	}

	#[test]
	fn test_decode_errors() {
		assert!(decode("xyz").is_err());
		assert!(decode("48656c6c6").is_err()); // odd length
	}

	#[test]
	fn test_roundtrip() {
		let data = b"Hello, World! \x00\x01\x02\xFF";
		let encoded = encode(data);
		let decoded = decode(&encoded).unwrap();
		assert_eq!(decoded, data);
	}
}
