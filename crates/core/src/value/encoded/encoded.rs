// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::util::CowVec;

/// A boxed values iterator.
pub type EncodedValuesIter = Box<dyn EncodedValuesIterator>;

pub trait EncodedValuesIterator: Iterator<Item = EncodedValues> {}

impl<I: Iterator<Item = EncodedValues>> EncodedValuesIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize)]
// bitvec:values:dynamic_encoded_values
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
}
