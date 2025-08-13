// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Blob;
use crate::{Error, Span, result::error::diagnostic::blob};

impl Blob {
	pub fn from_hex(span: impl Span) -> Result<Self, Error> {
		let hex_str = span.fragment();
		let clean_hex = if hex_str.starts_with("0x")
			|| hex_str.starts_with("0X")
		{
			&hex_str[2..]
		} else {
			hex_str
		};

		match hex::decode(clean_hex) {
			Ok(bytes) => Ok(Blob::new(bytes)),
			Err(_) => Err(Error(blob::invalid_hex_string(
				span.to_owned(),
			))),
		}
	}

	pub fn to_hex(&self) -> String {
		format!("0x{}", hex::encode(self.as_bytes()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::OwnedSpan;

	#[test]
	fn test_from_hex() {
		let blob = Blob::from_hex(OwnedSpan::testing("48656c6c6f"))
			.unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_hex_with_prefix() {
		let blob = Blob::from_hex(OwnedSpan::testing("0x48656c6c6f"))
			.unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");

		let blob = Blob::from_hex(OwnedSpan::testing("0X48656c6c6f"))
			.unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_hex_empty() {
		let blob = Blob::from_hex(OwnedSpan::testing("")).unwrap();
		assert_eq!(blob.as_bytes(), b"");
	}

	#[test]
	fn test_from_hex_invalid() {
		let result = Blob::from_hex(OwnedSpan::testing("xyz"));
		assert!(result.is_err());

		let result = Blob::from_hex(OwnedSpan::testing("48656c6c6")); // odd length
		assert!(result.is_err());
	}

	#[test]
	fn test_to_hex() {
		let blob = Blob::new(b"Hello".to_vec());
		assert_eq!(blob.to_hex(), "0x48656c6c6f");
	}

	#[test]
	fn test_hex_roundtrip() {
		let original = b"Hello, World! \x00\x01\x02\xFF";
		let blob = Blob::new(original.to_vec());
		let hex_str = blob.to_hex();
		let decoded =
			Blob::from_hex(OwnedSpan::testing(&hex_str)).unwrap();
		assert_eq!(decoded.as_bytes(), original);
	}
}
