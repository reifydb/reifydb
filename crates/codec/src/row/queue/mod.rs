// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The queue storage family: the source header plus a `not_before` instant that gates when an item
//! becomes due. Absence is a flag bit, not a sentinel instant, so a due-now item stays due-now.

use std::ops::Deref;

use reifydb_value::value::datetime::DateTime;

use crate::row::{
	bytes::{
		EncodedBytes, EncodedRowBuilder, QUEUE_HEADER_SIZE, RowBuilder, read_created_at, read_defined_at,
		read_fingerprint, read_not_before, read_updated_at, sealed::Sealed, write_fingerprint,
		write_not_before, write_storage_time, write_timestamps,
	},
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
		read_fingerprint(&self.0)
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

/// The write side of the queue family: a buffer whose source header and `not_before` slot are
/// already reserved, which freezes into an [`EncodedQueueRow`] and never into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedQueueRowBuilder(EncodedRowBuilder);

impl EncodedQueueRowBuilder {
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
	pub fn created_at(&self) -> DateTime {
		read_created_at(self.as_slice())
	}

	#[inline]
	pub fn updated_at(&self) -> DateTime {
		read_updated_at(self.as_slice())
	}

	pub fn set_timestamps(&mut self, created_at: DateTime, updated_at: DateTime) {
		write_timestamps(self.as_mut_slice(), created_at, updated_at);
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_storage_time(self.as_mut_slice(), time);
	}

	#[inline]
	pub fn not_before(&self) -> Option<DateTime> {
		read_not_before(self.as_slice())
	}

	pub fn set_not_before(&mut self, not_before: DateTime) {
		write_not_before(self.as_mut_slice(), not_before);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), QUEUE_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[QUEUE_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedQueueRow {
		EncodedQueueRow(self.0.freeze())
	}
}

impl Sealed for EncodedQueueRowBuilder {
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

impl EncodedQueueRow {
	pub fn thaw(self) -> EncodedQueueRowBuilder {
		EncodedQueueRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedQueueRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
