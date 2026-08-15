// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The queue attempt storage family: the source header plus the three fixed-width facts every
//! attempt carries - outcome, lost and finished_at. They sit in the header rather than the body so a
//! sweeper can classify an attempt without decoding the variable-length worker and response text.

use std::ops::Deref;

use reifydb_value::value::datetime::DateTime;

use crate::row::{
	bytes::{
		EncodedBytes, EncodedRowBuilder, QUEUE_ATTEMPT_HEADER_SIZE, RowBuilder, read_created_at,
		read_defined_at, read_fingerprint, read_finished_at, read_lost, read_outcome, read_updated_at,
		sealed::Sealed, write_finished_at, write_fingerprint, write_lost, write_outcome, write_storage_time,
		write_timestamps,
	},
	shape::fingerprint::RowShapeFingerprint,
};

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedQueueAttemptRow(EncodedBytes);

impl EncodedQueueAttemptRow {
	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedQueueAttemptRow is repr(transparent) over EncodedBytes, so the pointer cast
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
	pub fn outcome(&self) -> u8 {
		read_outcome(&self.0)
	}

	#[inline]
	pub fn lost(&self) -> bool {
		read_lost(&self.0)
	}

	#[inline]
	pub fn finished_at(&self) -> DateTime {
		read_finished_at(&self.0)
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(&self.0, QUEUE_ATTEMPT_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.0[QUEUE_ATTEMPT_HEADER_SIZE..]
	}
}

impl From<EncodedQueueAttemptRow> for EncodedBytes {
	fn from(row: EncodedQueueAttemptRow) -> Self {
		row.0
	}
}

impl From<EncodedBytes> for EncodedQueueAttemptRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}

/// The write side of the queue attempt family: a buffer whose source header and outcome, lost and
/// finished_at slots are already reserved, which freezes into an [`EncodedQueueAttemptRow`] and never
/// into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedQueueAttemptRowBuilder(EncodedRowBuilder);

impl EncodedQueueAttemptRowBuilder {
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
	pub fn outcome(&self) -> u8 {
		read_outcome(self.as_slice())
	}

	pub fn set_outcome(&mut self, outcome: u8) {
		write_outcome(self.as_mut_slice(), outcome);
	}

	#[inline]
	pub fn lost(&self) -> bool {
		read_lost(self.as_slice())
	}

	pub fn set_lost(&mut self, lost: bool) {
		write_lost(self.as_mut_slice(), lost);
	}

	#[inline]
	pub fn finished_at(&self) -> DateTime {
		read_finished_at(self.as_slice())
	}

	pub fn set_finished_at(&mut self, finished_at: DateTime) {
		write_finished_at(self.as_mut_slice(), finished_at);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), QUEUE_ATTEMPT_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[QUEUE_ATTEMPT_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedQueueAttemptRow {
		EncodedQueueAttemptRow(self.0.freeze())
	}
}

impl Sealed for EncodedQueueAttemptRowBuilder {
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

impl EncodedQueueAttemptRow {
	pub fn thaw(self) -> EncodedQueueAttemptRowBuilder {
		EncodedQueueAttemptRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedQueueAttemptRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
