// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The operator family: a row carrying nothing but the instant it belongs to, so offset zero is an
//! 8-byte time slot and `DateTime::MAX` stands for a row that has no time at all.

use std::ops::Deref;

use reifydb_value::{byte_size::ByteSize, encoding::LeBytes, util::cowvec::CowVec, value::datetime::DateTime};

use crate::row::bytes::{EncodedBytes, EncodedRowBuilder, RowBuilder, read_defined_at, sealed::Sealed};

pub mod state;

const TIME_OFFSET: usize = 0;

pub const OPERATOR_HEADER_SIZE: usize = TIME_OFFSET + DateTime::ENCODED_SIZE;

#[inline]
pub fn read_time(buf: &[u8]) -> Option<DateTime> {
	let time = DateTime::from_le_bytes(
		buf[TIME_OFFSET..OPERATOR_HEADER_SIZE].try_into().expect("the operator header is length-checked"),
	);
	(time != DateTime::MAX).then_some(time)
}

#[inline]
pub fn write_time(buf: &mut [u8], time: DateTime) {
	buf[TIME_OFFSET..OPERATOR_HEADER_SIZE].copy_from_slice(&time.to_le_bytes());
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedOperatorRow(EncodedBytes);

impl EncodedOperatorRow {
	pub fn new(body: &[u8], time: DateTime) -> Self {
		let mut buffer = Vec::with_capacity(OPERATOR_HEADER_SIZE + body.len());
		buffer.extend_from_slice(&time.to_le_bytes());
		buffer.extend_from_slice(body);
		Self(EncodedBytes(CowVec::new(buffer)))
	}

	pub fn timeless(body: &[u8]) -> Self {
		Self::new(body, DateTime::MAX)
	}

	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedOperatorRow is repr(transparent) over EncodedBytes, so the pointer cast
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
	pub fn row_time(&self) -> Option<DateTime> {
		read_time(&self.0)
	}

	#[inline]
	pub fn time(&self) -> DateTime {
		DateTime::from_le_bytes(
			self.0[TIME_OFFSET..OPERATOR_HEADER_SIZE].try_into().expect("the header is length-checked"),
		)
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_time(self.0.make_mut(), time);
	}

	pub fn body(&self) -> &[u8] {
		&self.0[OPERATOR_HEADER_SIZE..]
	}

	pub fn body_mut(&mut self) -> &mut [u8] {
		&mut self.0.make_mut()[OPERATOR_HEADER_SIZE..]
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

impl From<EncodedBytes> for EncodedOperatorRow {
	fn from(bytes: EncodedBytes) -> Self {
		Self(bytes)
	}
}

impl From<EncodedOperatorRow> for EncodedBytes {
	fn from(row: EncodedOperatorRow) -> Self {
		row.0
	}
}

/// The write side of the operator family: a buffer already carrying a time header, which freezes
/// into an [`EncodedOperatorRow`] and never into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedOperatorRowBuilder(EncodedRowBuilder);

impl EncodedOperatorRowBuilder {
	pub(crate) fn wrap(builder: EncodedRowBuilder) -> Self {
		Self(builder)
	}

	#[inline]
	pub fn row_time(&self) -> Option<DateTime> {
		read_time(self.as_slice())
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_time(self.as_mut_slice(), time);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), OPERATOR_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[OPERATOR_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedOperatorRow {
		EncodedOperatorRow(self.0.freeze())
	}
}

impl Sealed for EncodedOperatorRowBuilder {
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

impl EncodedOperatorRow {
	pub fn thaw(self) -> EncodedOperatorRowBuilder {
		EncodedOperatorRowBuilder(self.0.thaw())
	}
}

impl Deref for EncodedOperatorRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
