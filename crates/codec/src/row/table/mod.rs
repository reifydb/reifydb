// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The table storage family: the source header of fingerprint, wall stamps, `#time` and flags. A
//! table row is addressed through its shape, so the header ends where the definedness bitvec begins.

use reifydb_value::value::datetime::DateTime;

use crate::row::{
	bytes::{
		EncodedBytes, SHAPE_HEADER_SIZE, read_created_at, read_defined_at, read_storage_time, read_updated_at,
	},
	shape::fingerprint::RowShapeFingerprint,
};

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedTableRow(EncodedBytes);

impl EncodedTableRow {
	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedTableRow is repr(transparent) over EncodedBytes, so the pointer cast preserves
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
		self.0.fingerprint()
	}

	#[inline]
	pub fn created_at(&self) -> DateTime {
		read_created_at(&self.0)
	}

	#[inline]
	pub fn updated_at(&self) -> DateTime {
		read_updated_at(&self.0)
	}

	#[inline]
	pub fn time(&self) -> Option<DateTime> {
		read_storage_time(&self.0)
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(&self.0, SHAPE_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.0[SHAPE_HEADER_SIZE..]
	}
}

impl From<EncodedTableRow> for EncodedBytes {
	fn from(row: EncodedTableRow) -> Self {
		row.0
	}
}

impl From<EncodedBytes> for EncodedTableRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}
