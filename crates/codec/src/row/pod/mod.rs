// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The pod storage family: an entry read and written whole, carrying no header at all, so the body
//! is the whole row and offset zero is payload rather than a fingerprint or a stamp.

use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::row::bytes::EncodedBytes;

pub const POD_HEADER_SIZE: usize = 0;

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPodRow(EncodedBytes);

impl EncodedPodRow {
	pub fn new(body: &[u8]) -> Self {
		Self(EncodedBytes(CowVec::new(body.to_vec())))
	}

	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedPodRow is repr(transparent) over EncodedBytes, so the pointer cast
		// preserves layout, and the returned reference borrows the same allocation for the same lifetime.
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

	pub fn body(&self) -> &[u8] {
		&self.0[POD_HEADER_SIZE..]
	}

	pub fn body_mut(&mut self) -> &mut [u8] {
		&mut self.0.make_mut()[POD_HEADER_SIZE..]
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
}

impl From<EncodedBytes> for EncodedPodRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}

impl From<EncodedPodRow> for EncodedBytes {
	fn from(row: EncodedPodRow) -> Self {
		row.0
	}
}
