// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The queue storage family: the source header plus a `not_before` instant that gates when an item
//! becomes due. Absence is a flag bit, not a sentinel instant, so a due-now item stays due-now.

use reifydb_value::value::datetime::DateTime;

use crate::row::{
	bytes::{EncodedBytes, QUEUE_HEADER_SIZE, read_created_at, read_defined_at, read_not_before, read_updated_at},
	shape::fingerprint::RowShapeFingerprint,
};

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedQueueRow(EncodedBytes);

impl EncodedQueueRow {
	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedQueueRow is repr(transparent) over EncodedBytes, so the pointer cast preserves
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
	pub fn not_before(&self) -> Option<DateTime> {
		read_not_before(&self.0)
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(&self.0, QUEUE_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.0[QUEUE_HEADER_SIZE..]
	}
}

impl From<EncodedQueueRow> for EncodedBytes {
	fn from(row: EncodedQueueRow) -> Self {
		row.0
	}
}

impl From<EncodedBytes> for EncodedQueueRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}
