// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::util::CowVec;

pub type EncodedIndexKeyIter = Box<dyn EncodedIndexKeyIterator>;

pub trait EncodedIndexKeyIterator: Iterator<Item = EncodedIndexKey> {}

impl<I: Iterator<Item = EncodedIndexKey>> EncodedIndexKeyIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncodedIndexKey(pub CowVec<u8>);

impl Deref for EncodedIndexKey {
	type Target = CowVec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedIndexKey {
	pub fn make_mut(&mut self) -> &mut [u8] {
		self.0.make_mut()
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		let byte = index / 8;
		let bit = index % 8;
		(self.0[byte] & (1 << bit)) != 0
	}

	pub(crate) fn set_valid(&mut self, index: usize, bitvec: bool) {
		let byte = index / 8;
		let bit = index % 8;
		if bitvec {
			self.0.make_mut()[byte] |= 1 << bit;
		} else {
			self.0.make_mut()[byte] &= !(1 << bit);
		}
	}

	/// Creates an EncodedIndexKey from a byte slice
	pub fn from_bytes(bytes: &[u8]) -> Self {
		Self(CowVec::new(bytes.to_vec()))
	}
}
