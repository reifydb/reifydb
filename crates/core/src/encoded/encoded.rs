// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_type::util::cowvec::CowVec;
use serde::{Deserialize, Serialize};

use super::schema::SchemaFingerprint;

/// Size of schema header (fingerprint) in bytes
pub const SCHEMA_HEADER_SIZE: usize = 8;

/// A boxed values iterator.
pub type EncodedValuesIter = Box<dyn EncodedValuesIterator>;

pub trait EncodedValuesIterator: Iterator<Item = EncodedValues> {}

impl<I: Iterator<Item = EncodedValues>> EncodedValuesIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize)]
// [schema_finger_print]:[bitvec]:[static_values]:[dynamic_values]
#[derive(PartialEq, Eq)]
pub struct EncodedValues(pub CowVec<u8>);

impl Deref for EncodedValues {
	type Target = CowVec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedValues {
	pub fn make_mut(&mut self) -> &mut [u8] {
		self.0.make_mut()
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		let byte = SCHEMA_HEADER_SIZE + index / 8;
		let bit = index % 8;
		(self.0[byte] & (1 << bit)) != 0
	}

	pub(crate) fn set_valid(&mut self, index: usize, valid: bool) {
		let byte = SCHEMA_HEADER_SIZE + index / 8;
		let bit = index % 8;
		if valid {
			self.0.make_mut()[byte] |= 1 << bit;
		} else {
			self.0.make_mut()[byte] &= !(1 << bit);
		}
	}

	/// Read the schema fingerprint from the header
	#[inline]
	pub fn fingerprint(&self) -> SchemaFingerprint {
		let bytes: [u8; 8] = self.0[0..8].try_into().unwrap();
		SchemaFingerprint::from_le_bytes(bytes)
	}

	/// Write the schema fingerprint to the header
	pub fn set_fingerprint(&mut self, fingerprint: SchemaFingerprint) {
		self.0.make_mut()[0..8].copy_from_slice(&fingerprint.to_le_bytes());
	}
}
