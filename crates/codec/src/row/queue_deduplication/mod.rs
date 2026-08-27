// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::row::{
	bytes::{
		EncodedBytes, EncodedRowBuilder, RowBuilder, read_created_at, read_deduplication_row_number,
		read_expires_at, read_fingerprint, read_updated_at, sealed::Sealed, write_deduplication_row_number,
		write_expires_at, write_fingerprint, write_storage_time, write_timestamps,
	},
	shape::fingerprint::RowShapeFingerprint,
};

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedQueueDeduplicationRow(EncodedBytes);

impl EncodedQueueDeduplicationRow {
	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedQueueDeduplicationRow is repr(transparent) over EncodedBytes, so the pointer cast
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
	pub fn row_number(&self) -> RowNumber {
		read_deduplication_row_number(&self.0)
	}

	#[inline]
	pub fn expires_at(&self) -> DateTime {
		read_expires_at(&self.0)
	}
}

impl From<EncodedQueueDeduplicationRow> for EncodedBytes {
	fn from(row: EncodedQueueDeduplicationRow) -> Self {
		row.0
	}
}

impl From<EncodedBytes> for EncodedQueueDeduplicationRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedQueueDeduplicationRowBuilder(EncodedRowBuilder);

impl EncodedQueueDeduplicationRowBuilder {
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
	pub fn row_number(&self) -> RowNumber {
		read_deduplication_row_number(self.as_slice())
	}

	pub fn set_row_number(&mut self, row_number: RowNumber) {
		write_deduplication_row_number(self.as_mut_slice(), row_number);
	}

	#[inline]
	pub fn expires_at(&self) -> DateTime {
		read_expires_at(self.as_slice())
	}

	pub fn set_expires_at(&mut self, expires_at: DateTime) {
		write_expires_at(self.as_mut_slice(), expires_at);
	}

	pub fn freeze(self) -> EncodedQueueDeduplicationRow {
		EncodedQueueDeduplicationRow(self.0.freeze())
	}
}

impl Sealed for EncodedQueueDeduplicationRowBuilder {
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

impl EncodedQueueDeduplicationRow {
	pub fn thaw(self) -> EncodedQueueDeduplicationRowBuilder {
		EncodedQueueDeduplicationRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedQueueDeduplicationRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
