// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The catalog storage family: a row whose header is the shape fingerprint and nothing else.

use std::ops::Deref;

use reifydb_value::{
	byte_size::ByteSize,
	error::{Error as ValueError, TypeError},
	util::cowvec::CowVec,
};
use thiserror::Error;

use crate::row::{
	bytes::{CATALOG_HEADER_SIZE, EncodedBytes, EncodedRowBuilder, RowBuilder, read_defined_at, sealed::Sealed},
	shape::fingerprint::RowShapeFingerprint,
};

const FINGERPRINT_OFFSET: usize = 0;

#[derive(Debug, Error, PartialEq)]
pub enum CatalogError {
	#[error("catalog row is {len} bytes, too short to carry the fingerprint header")]
	Truncated {
		len: usize,
	},
}

impl From<CatalogError> for ValueError {
	fn from(err: CatalogError) -> Self {
		TypeError::SerdeDeserialize {
			message: err.to_string(),
		}
		.into()
	}
}

#[inline]
pub fn read_fingerprint(buf: &[u8]) -> RowShapeFingerprint {
	RowShapeFingerprint::from_le_bytes(
		buf[FINGERPRINT_OFFSET..CATALOG_HEADER_SIZE].try_into().expect("the catalog header is length-checked"),
	)
}

#[inline]
pub fn write_fingerprint(buf: &mut [u8], fingerprint: RowShapeFingerprint) {
	buf[FINGERPRINT_OFFSET..CATALOG_HEADER_SIZE].copy_from_slice(&fingerprint.to_le_bytes());
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedCatalogRow(EncodedBytes);

impl EncodedCatalogRow {
	pub fn new(body: &[u8], fingerprint: RowShapeFingerprint) -> Self {
		let mut buffer = Vec::with_capacity(CATALOG_HEADER_SIZE + body.len());
		buffer.extend_from_slice(&fingerprint.to_le_bytes());
		buffer.extend_from_slice(body);
		Self(EncodedBytes(CowVec::new(buffer)))
	}

	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedCatalogRow is repr(transparent) over EncodedBytes, so the pointer cast preserves
		// layout, and the returned reference borrows the same allocation for the same lifetime.
		unsafe { &*(bytes as *const EncodedBytes as *const Self) }
	}

	pub fn bytes(&self) -> &EncodedBytes {
		&self.0
	}

	pub fn as_slice(&self) -> &[u8] {
		self.0.as_slice()
	}

	pub fn into_bytes(self) -> EncodedBytes {
		self.0
	}

	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		read_fingerprint(&self.0)
	}

	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		write_fingerprint(self.0.make_mut(), fingerprint);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(&self.0, CATALOG_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.0[CATALOG_HEADER_SIZE..]
	}

	pub fn body_mut(&mut self) -> &mut [u8] {
		&mut self.0.make_mut()[CATALOG_HEADER_SIZE..]
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.body().is_empty()
	}

	pub fn byte_size(&self) -> ByteSize {
		ByteSize::from(self.0.len() as u64)
	}

	pub fn thaw(self) -> EncodedCatalogRowBuilder {
		EncodedCatalogRowBuilder(self.0.thaw())
	}
}

impl TryFrom<EncodedBytes> for EncodedCatalogRow {
	type Error = CatalogError;

	fn try_from(bytes: EncodedBytes) -> Result<Self, Self::Error> {
		if bytes.len() < CATALOG_HEADER_SIZE {
			return Err(CatalogError::Truncated {
				len: bytes.len(),
			});
		}
		Ok(Self(bytes))
	}
}

impl From<EncodedCatalogRow> for EncodedBytes {
	fn from(row: EncodedCatalogRow) -> Self {
		row.0
	}
}

/// The write side of the catalog family: a buffer already carrying a fingerprint header, which
/// freezes into an [`EncodedCatalogRow`] and never into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedCatalogRowBuilder(EncodedRowBuilder);

impl EncodedCatalogRowBuilder {
	pub(crate) fn wrap(builder: EncodedRowBuilder) -> Self {
		Self(builder)
	}

	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		read_fingerprint(self.as_slice())
	}

	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		write_fingerprint(self.as_mut_slice(), fingerprint);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), CATALOG_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[CATALOG_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedCatalogRow {
		EncodedCatalogRow(self.0.freeze())
	}
}

impl Sealed for EncodedCatalogRowBuilder {
	fn buffer(&self) -> &Vec<u8> {
		self.0.buffer()
	}

	fn buffer_mut(&mut self) -> &mut Vec<u8> {
		self.0.buffer_mut()
	}

	fn take_buffer(self) -> Vec<u8> {
		self.0.take_buffer()
	}
}

impl Deref for EncodedCatalogRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
