// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::util::cowvec::CowVec;
use serde::{Deserialize, Serialize};

use crate::encoded::shape::fingerprint::RowShapeFingerprint;

/// Size of shape header (fingerprint) in bytes
pub const SHAPE_HEADER_SIZE: usize = 8;

/// A boxed values iterator.
pub type EncodedRowIter = Box<dyn EncodedRowIterator>;

pub trait EncodedRowIterator: Iterator<Item = EncodedRow> {}

impl<I: Iterator<Item = EncodedRow>> EncodedRowIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize)]
// [shape_finger_print]:[bitvec]:[static_values]:[dynamic_values]
#[derive(PartialEq, Eq)]
pub struct EncodedRow(pub CowVec<u8>);

impl Deref for EncodedRow {
	type Target = CowVec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedRow {
	pub fn make_mut(&mut self) -> &mut [u8] {
		self.0.make_mut()
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		let byte = SHAPE_HEADER_SIZE + index / 8;
		let bit = index % 8;
		(self.0[byte] & (1 << bit)) != 0
	}

	pub(crate) fn set_valid(&mut self, index: usize, valid: bool) {
		let byte = SHAPE_HEADER_SIZE + index / 8;
		let bit = index % 8;
		if valid {
			self.0.make_mut()[byte] |= 1 << bit;
		} else {
			self.0.make_mut()[byte] &= !(1 << bit);
		}
	}

	/// Read the shape fingerprint from the header
	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		let bytes: [u8; 8] = self.0[0..8].try_into().unwrap();
		RowShapeFingerprint::from_le_bytes(bytes)
	}

	/// Write the shape fingerprint to the header
	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		self.0.make_mut()[0..8].copy_from_slice(&fingerprint.to_le_bytes());
	}
}
