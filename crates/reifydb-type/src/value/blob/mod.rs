// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

pub mod base64;
pub mod hex;
pub mod utf8;

use std::{
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Serialize};

/// A binary large object (BLOB) wrapper type
#[repr(transparent)]
#[derive(
	Default,
	Debug,
	Clone,
	PartialEq,
	Eq,
	Hash,
	PartialOrd,
	Ord,
	Serialize,
	Deserialize,
)]
pub struct Blob(Vec<u8>);

impl Blob {
	/// Create a new BLOB from raw bytes
	pub fn new(bytes: Vec<u8>) -> Self {
		Self(bytes)
	}

	pub fn empty() -> Self {
		Self(Vec::with_capacity(0))
	}

	/// Create a BLOB from a byte slice
	pub fn from_slice(bytes: &[u8]) -> Self {
		Self(Vec::from(bytes.to_vec()))
	}

	/// Get the raw bytes
	pub fn as_bytes(&self) -> &[u8] {
		&self.0
	}

	/// Get the length in bytes
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Check if the BLOB is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Convert into the inner bytes
	pub fn into_bytes(self) -> Vec<u8> {
		self.0.to_vec()
	}
}

impl Deref for Blob {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Vec<u8>> for Blob {
	fn from(bytes: Vec<u8>) -> Self {
		Self::new(bytes)
	}
}

impl From<&[u8]> for Blob {
	fn from(bytes: &[u8]) -> Self {
		Self::from_slice(bytes)
	}
}

impl From<Blob> for Vec<u8> {
	fn from(blob: Blob) -> Self {
		blob.into_bytes()
	}
}

impl Display for Blob {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "0x{}", hex::encode(self.as_bytes()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_blob_from_bytes() {
		let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
		let blob = Blob::new(data.clone());
		assert_eq!(blob.as_bytes(), &data);
		assert_eq!(blob.len(), 4);
		assert!(!blob.is_empty());
	}

	#[test]
	fn test_blob_from_slice() {
		let data = [0xCA, 0xFE, 0xBA, 0xBE];
		let blob = Blob::from_slice(&data);
		assert_eq!(blob.as_bytes(), &data);
	}

	#[test]
	fn test_blob_deref() {
		let blob = Blob::new(vec![1, 2, 3]);
		let bytes: &[u8] = &blob;
		assert_eq!(bytes, &[1, 2, 3]);
	}

	#[test]
	fn test_blob_conversions() {
		let data = vec![0xFF, 0x00, 0xFF];
		let blob = Blob::from(data.clone());
		let bytes: Vec<u8> = blob.into();
		assert_eq!(bytes, data);
	}

	#[test]
	fn test_blob_display() {
		let blob = Blob::new(vec![0xDE, 0xAD, 0xBE, 0xEF]);
		assert_eq!(format!("{}", blob), "0xdeadbeef");

		let empty_blob = Blob::new(vec![]);
		assert_eq!(format!("{}", empty_blob), "0x");

		let single_byte_blob = Blob::new(vec![0xFF]);
		assert_eq!(format!("{}", single_byte_blob), "0xff");
	}
}
