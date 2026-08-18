// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A variable-width envelope carried inside a pod body: one leading flags byte followed only by the
//! eight-byte fields the writer actually set, in flag-bit order, so a row pays for a stamp or a
//! fingerprint only when it carries one and presence is the flag rather than a sentinel value.

use reifydb_value::{
	encoding::LeBytes,
	error::{Error as ValueError, TypeError},
	util::cowvec::CowVec,
	value::datetime::DateTime,
};
use thiserror::Error;

use crate::row::{bytes::EncodedBytes, pod::EncodedPodRow, shape::fingerprint::RowShapeFingerprint};

pub const HAS_CREATED_AT: u8 = 1 << 0;

pub const HAS_UPDATED_AT: u8 = 1 << 1;

pub const HAS_TIME: u8 = 1 << 2;

pub const HAS_FINGERPRINT: u8 = 1 << 3;

pub const ENVELOPE_FLAGS_SIZE: usize = 1;

pub const ENVELOPE_FIELD_SIZE: usize = DateTime::ENCODED_SIZE;

#[inline]
pub const fn header_size(flags: u8) -> usize {
	ENVELOPE_FLAGS_SIZE + ENVELOPE_FIELD_SIZE * flags.count_ones() as usize
}

#[inline]
const fn field_offset(flags: u8, bit: u8) -> usize {
	ENVELOPE_FLAGS_SIZE + ENVELOPE_FIELD_SIZE * (flags & (bit - 1)).count_ones() as usize
}

#[derive(Debug, Error, PartialEq)]
pub enum EnvelopeError {
	#[error("envelope row is {len} bytes, too short for the {required} byte header its flags declare")]
	Truncated {
		len: usize,
		required: usize,
	},
}

impl From<EnvelopeError> for ValueError {
	fn from(err: EnvelopeError) -> Self {
		TypeError::SerdeDeserialize {
			message: err.to_string(),
		}
		.into()
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope(EncodedBytes);

impl Envelope {
	pub fn try_view(row: &EncodedPodRow) -> Result<&Self, EnvelopeError> {
		let bytes = row.bytes();
		let len = bytes.len();
		if len < ENVELOPE_FLAGS_SIZE {
			return Err(EnvelopeError::Truncated {
				len,
				required: ENVELOPE_FLAGS_SIZE,
			});
		}
		let required = header_size(bytes[0]);
		if len < required {
			return Err(EnvelopeError::Truncated {
				len,
				required,
			});
		}
		// SAFETY: Envelope is repr(transparent) over EncodedBytes, so the pointer cast preserves layout,
		// and the returned reference borrows the same allocation for the same lifetime.
		Ok(unsafe { &*(bytes as *const EncodedBytes as *const Self) })
	}

	#[inline]
	pub fn flags(&self) -> u8 {
		self.0[0]
	}

	#[inline]
	pub fn header_size(&self) -> usize {
		header_size(self.flags())
	}

	#[inline]
	pub fn created_at(&self) -> Option<DateTime> {
		self.field(HAS_CREATED_AT).map(DateTime::from_le_bytes)
	}

	#[inline]
	pub fn updated_at(&self) -> Option<DateTime> {
		self.field(HAS_UPDATED_AT).map(DateTime::from_le_bytes)
	}

	#[inline]
	pub fn time(&self) -> Option<DateTime> {
		self.field(HAS_TIME).map(DateTime::from_le_bytes)
	}

	#[inline]
	pub fn fingerprint(&self) -> Option<RowShapeFingerprint> {
		self.field(HAS_FINGERPRINT).map(RowShapeFingerprint::from_le_bytes)
	}

	#[inline]
	pub fn body(&self) -> &[u8] {
		&self.0[self.header_size()..]
	}

	#[inline]
	fn field(&self, bit: u8) -> Option<[u8; ENVELOPE_FIELD_SIZE]> {
		let flags = self.flags();
		if flags & bit == 0 {
			return None;
		}
		let offset = field_offset(flags, bit);
		Some(self.0[offset..offset + ENVELOPE_FIELD_SIZE]
			.try_into()
			.expect("the envelope header is length-checked"))
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnvelopeBuilder {
	created_at: Option<DateTime>,
	updated_at: Option<DateTime>,
	time: Option<DateTime>,
	fingerprint: Option<RowShapeFingerprint>,
}

impl EnvelopeBuilder {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn created_at(mut self, created_at: DateTime) -> Self {
		self.created_at = Some(created_at);
		self
	}

	pub fn updated_at(mut self, updated_at: DateTime) -> Self {
		self.updated_at = Some(updated_at);
		self
	}

	pub fn time(mut self, time: DateTime) -> Self {
		self.time = Some(time);
		self
	}

	pub fn fingerprint(mut self, fingerprint: RowShapeFingerprint) -> Self {
		self.fingerprint = Some(fingerprint);
		self
	}

	pub fn flags(&self) -> u8 {
		let mut flags = 0u8;
		if self.created_at.is_some() {
			flags |= HAS_CREATED_AT;
		}
		if self.updated_at.is_some() {
			flags |= HAS_UPDATED_AT;
		}
		if self.time.is_some() {
			flags |= HAS_TIME;
		}
		if self.fingerprint.is_some() {
			flags |= HAS_FINGERPRINT;
		}
		flags
	}

	pub fn build(self, body: &[u8]) -> EncodedPodRow {
		let flags = self.flags();
		let mut buffer = Vec::with_capacity(header_size(flags) + body.len());
		buffer.push(flags);
		for stamp in [self.created_at, self.updated_at, self.time].into_iter().flatten() {
			buffer.extend_from_slice(&stamp.to_le_bytes());
		}
		if let Some(fingerprint) = self.fingerprint {
			buffer.extend_from_slice(&fingerprint.to_le_bytes());
		}
		buffer.extend_from_slice(body);
		EncodedPodRow::from(EncodedBytes(CowVec::new(buffer)))
	}
}
