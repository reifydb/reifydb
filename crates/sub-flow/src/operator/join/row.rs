// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{bytes::EncodedBytes, shape::fingerprint::RowShapeFingerprint};
use reifydb_value::{encoding::LeBytes, util::cowvec::CowVec, value::datetime::DateTime};

const FINGERPRINT_OFFSET: usize = 0;
const STAMP_OFFSET: usize = FINGERPRINT_OFFSET + 8;
const FLAGS_OFFSET: usize = STAMP_OFFSET + DateTime::ENCODED_SIZE;
pub(crate) const JOIN_STATE_HEADER_SIZE: usize = FLAGS_OFFSET + 1;

const HAS_TIME: u8 = 1 << 0;

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinStateRow(EncodedBytes);

impl JoinStateRow {
	pub(crate) fn new(
		body: &[u8],
		fingerprint: RowShapeFingerprint,
		stamp: DateTime,
		time: Option<DateTime>,
	) -> Self {
		let mut buffer = Vec::with_capacity(JOIN_STATE_HEADER_SIZE + body.len());
		buffer.extend_from_slice(&fingerprint.to_le_bytes());
		buffer.extend_from_slice(&time.unwrap_or(stamp).to_le_bytes());
		buffer.push(if time.is_some() {
			HAS_TIME
		} else {
			0
		});
		buffer.extend_from_slice(body);
		Self(EncodedBytes(CowVec::new(buffer)))
	}

	pub(crate) fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: JoinStateRow is repr(transparent) over EncodedBytes, so the pointer cast preserves
		// layout, and the returned reference borrows the same allocation for the same lifetime.
		unsafe { &*(bytes as *const EncodedBytes as *const Self) }
	}

	pub(crate) fn as_slice(&self) -> &[u8] {
		self.0.as_slice()
	}

	#[inline]
	pub(crate) fn fingerprint(&self) -> RowShapeFingerprint {
		RowShapeFingerprint::new(u64::from_le_bytes(
			self.0[FINGERPRINT_OFFSET..STAMP_OFFSET]
				.try_into()
				.expect("the join state header is fixed width"),
		))
	}

	#[inline]
	pub(crate) fn stamp(&self) -> DateTime {
		DateTime::from_le_bytes(
			self.0[STAMP_OFFSET..FLAGS_OFFSET].try_into().expect("the join state header is fixed width"),
		)
	}

	#[inline]
	pub(crate) fn time(&self) -> Option<DateTime> {
		(self.0[FLAGS_OFFSET] & HAS_TIME != 0).then(|| self.stamp())
	}

	#[inline]
	pub(crate) fn body(&self) -> &[u8] {
		&self.0[JOIN_STATE_HEADER_SIZE..]
	}

	pub(crate) fn body_bytes(&self) -> EncodedBytes {
		EncodedBytes(CowVec::new(self.body().to_vec()))
	}
}
