// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::Blob;
use crate::{
	error::{Error, diagnostic::blob},
	fragment::Fragment,
	util::base58,
};

impl Blob {
	pub fn from_b58(fragment: Fragment) -> Result<Self, Error> {
		let fragment = fragment;
		let b58_str = fragment.text();
		match base58::decode(b58_str) {
			Ok(bytes) => Ok(Blob::new(bytes)),
			Err(_) => Err(Error(blob::invalid_base58_string(fragment))),
		}
	}

	pub fn to_b58(&self) -> String {
		base58::encode(self.as_bytes())
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::fragment::Fragment;

	#[test]
	fn test_from_b58() {
		let blob = Blob::from_b58(Fragment::testing("9Ajdvzr")).unwrap();
		assert_eq!(blob.as_bytes(), b"Hello");
	}

	#[test]
	fn test_from_b58_empty() {
		let blob = Blob::from_b58(Fragment::testing("")).unwrap();
		assert_eq!(blob.as_bytes(), b"");
	}

	#[test]
	fn test_from_b58_invalid() {
		// '0', 'O', 'I', 'l' are not in base58 alphabet
		let result = Blob::from_b58(Fragment::testing("0invalid"));
		assert!(result.is_err());

		let result = Blob::from_b58(Fragment::testing("Oops"));
		assert!(result.is_err());

		let result = Blob::from_b58(Fragment::testing("Invalid!"));
		assert!(result.is_err());
	}

	#[test]
	fn test_to_b58() {
		let blob = Blob::new(b"Hello".to_vec());
		assert_eq!(blob.to_b58(), "9Ajdvzr");
	}

	#[test]
	fn test_to_b58_empty() {
		let blob = Blob::new(vec![]);
		assert_eq!(blob.to_b58(), "");
	}

	#[test]
	fn test_b58_roundtrip() {
		let original = b"Hello, World! \x00\x01\x02\xFF";
		let blob = Blob::new(original.to_vec());
		let b58_str = blob.to_b58();
		let decoded = Blob::from_b58(Fragment::testing(&b58_str)).unwrap();
		assert_eq!(decoded.as_bytes(), original);
	}

	#[test]
	fn test_b58_binary_data() {
		let data = vec![0xde, 0xad, 0xbe, 0xef];
		let blob = Blob::new(data.clone());
		let b58_str = blob.to_b58();
		let decoded = Blob::from_b58(Fragment::testing(&b58_str)).unwrap();
		assert_eq!(decoded.as_bytes(), &data);
	}

	#[test]
	fn test_b58_leading_zeros() {
		// Leading zero bytes become leading '1's
		let data = vec![0, 0, 1];
		let blob = Blob::new(data.clone());
		let b58_str = blob.to_b58();
		assert_eq!(b58_str, "112");
		let decoded = Blob::from_b58(Fragment::testing(&b58_str)).unwrap();
		assert_eq!(decoded.as_bytes(), &data);
	}
}
