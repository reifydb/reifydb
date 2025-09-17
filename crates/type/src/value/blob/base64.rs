// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::Blob;
use crate::{Error, IntoFragment, error::diagnostic::blob, util::base64::engine::general_purpose};

impl Blob {
	pub fn from_b64<'a>(fragment: impl IntoFragment<'a>) -> Result<Self, Error> {
		let fragment = fragment.into_fragment();
		let b64_str = fragment.text();
		// Try standard base64 first, then without padding if it fails
		match general_purpose::STANDARD.decode(b64_str) {
			Ok(bytes) => Ok(Blob::new(bytes)),
			Err(_) => {
				// Try without padding
				match general_purpose::STANDARD_NO_PAD.decode(b64_str) {
					Ok(bytes) => Ok(Blob::new(bytes)),
					Err(_) => Err(Error(blob::invalid_base64_string(fragment))),
				}
			}
		}
	}

	pub fn from_b64url<'a>(fragment: impl IntoFragment<'a>) -> Result<Self, Error> {
		let fragment = fragment.into_fragment();
		let b64url_str = fragment.text();
		match general_purpose::URL_SAFE_NO_PAD.decode(b64url_str) {
			Ok(bytes) => Ok(Blob::new(bytes)),
			Err(_) => Err(Error(blob::invalid_base64url_string(fragment))),
		}
	}

	pub fn to_b64(&self) -> String {
		general_purpose::STANDARD.encode(self.as_bytes())
	}

	pub fn to_b64url(&self) -> String {
		general_purpose::URL_SAFE_NO_PAD.encode(self.as_bytes())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::OwnedFragment;

	#[test]
	fn test_from_b64() {
		let blob = Blob::from_b64(OwnedFragment::testing("SGVsbG8=")).unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_b64_no_padding() {
		let blob = Blob::from_b64(OwnedFragment::testing("SGVsbG8")).unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_b64_empty() {
		let blob = Blob::from_b64(OwnedFragment::testing("")).unwrap();
		assert_eq!(blob.as_bytes(), b"");
	}

	#[test]
	fn test_from_b64_invalid() {
		let result = Blob::from_b64(OwnedFragment::testing("!!!invalid!!!"));
		assert!(result.is_err());
	}

	#[test]
	fn test_from_b64url() {
		let blob = Blob::from_b64url(OwnedFragment::testing("SGVsbG8")).unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_b64url_invalid() {
		let result = Blob::from_b64url(OwnedFragment::testing("SGVsbG8=")); // padding not allowed in URL-safe
		assert!(result.is_err());
	}

	#[test]
	fn test_to_b64() {
		let blob = Blob::new(b"Hello".to_vec());
		assert_eq!(blob.to_b64(), "SGVsbG8=");
	}

	#[test]
	fn test_to_b64url() {
		let blob = Blob::new(b"Hello".to_vec());
		assert_eq!(blob.to_b64url(), "SGVsbG8");
	}

	#[test]
	fn test_b64_roundtrip() {
		let original = b"Hello, World! \x00\x01\x02\xFF";
		let blob = Blob::new(original.to_vec());
		let b64_str = blob.to_b64();
		let decoded = Blob::from_b64(OwnedFragment::testing(&b64_str)).unwrap();
		assert_eq!(decoded.as_bytes(), original);
	}

	#[test]
	fn test_b64url_roundtrip() {
		let original = b"Hello, World! \x00\x01\x02\xFF";
		let blob = Blob::new(original.to_vec());
		let b64url_str = blob.to_b64url();
		let decoded = Blob::from_b64url(OwnedFragment::testing(&b64url_str)).unwrap();
		assert_eq!(decoded.as_bytes(), original);
	}
}
