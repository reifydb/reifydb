// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The pod storage family: an entry read and written whole, carrying no header at all, so the body
//! is the whole row and offset zero is payload rather than a fingerprint or a stamp.

use std::ops::Deref;

use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::row::bytes::{EncodedBytes, EncodedRowBuilder, RowBuilder, read_defined_at, sealed::Sealed};

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

/// The write side of the pod family: a buffer with no header at all, so offset zero is already
/// payload and no stamp or fingerprint may be written into it.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPodRowBuilder(EncodedRowBuilder);

impl EncodedPodRowBuilder {
	pub(crate) fn wrap(builder: EncodedRowBuilder) -> Self {
		Self(builder)
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), POD_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[POD_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedPodRow {
		EncodedPodRow(self.0.freeze())
	}
}

impl Sealed for EncodedPodRowBuilder {
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

impl EncodedPodRow {
	pub fn thaw(self) -> EncodedPodRowBuilder {
		EncodedPodRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedPodRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
