// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The series storage family: the source header, byte for byte as the table family. The key repeats
//! in field 0 and again in `#time`; deduping it against `SeriesRowKey` is deferred, not rejected.

use std::ops::Deref;

use reifydb_value::value::datetime::DateTime;

use crate::row::{
	bytes::{
		EncodedBytes, EncodedRowBuilder, RowBuilder, SHAPE_HEADER_SIZE, SourceRowBuilder, read_created_at,
		read_defined_at, read_fingerprint, read_storage_time, read_updated_at, sealed::Sealed,
		write_fingerprint, write_storage_time, write_timestamps,
	},
	shape::fingerprint::RowShapeFingerprint,
};

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedSeriesRow(EncodedBytes);

impl EncodedSeriesRow {
	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedSeriesRow is repr(transparent) over EncodedBytes, so the pointer cast preserves
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

impl From<EncodedSeriesRow> for EncodedBytes {
	fn from(row: EncodedSeriesRow) -> Self {
		row.0
	}
}

impl From<EncodedBytes> for EncodedSeriesRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}

/// The write side of the series family: a buffer whose source header is already reserved, which
/// freezes into an [`EncodedSeriesRow`] and never into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedSeriesRowBuilder(EncodedRowBuilder);

impl EncodedSeriesRowBuilder {
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

	#[inline]
	pub fn time(&self) -> Option<DateTime> {
		read_storage_time(self.as_slice())
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_storage_time(self.as_mut_slice(), time);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), SHAPE_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[SHAPE_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedSeriesRow {
		EncodedSeriesRow(self.0.freeze())
	}
}

impl SourceRowBuilder for EncodedSeriesRowBuilder {
	fn set_timestamps(&mut self, created_at: DateTime, updated_at: DateTime) {
		EncodedSeriesRowBuilder::set_timestamps(self, created_at, updated_at);
	}

	fn set_time(&mut self, time: DateTime) {
		EncodedSeriesRowBuilder::set_time(self, time);
	}
}

impl Sealed for EncodedSeriesRowBuilder {
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

impl EncodedSeriesRow {
	pub fn thaw(self) -> EncodedSeriesRowBuilder {
		EncodedSeriesRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedSeriesRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
