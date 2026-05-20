// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Borrow,
	cmp::Ordering,
	fmt,
	hash::{Hash, Hasher},
	mem,
	ops::Deref,
};

use serde::{
	de::{Deserialize, Deserializer},
	ser::{Serialize, Serializer},
};

pub type EncodedIndexKeyIter = Box<dyn EncodedIndexKeyIterator>;

pub trait EncodedIndexKeyIterator: Iterator<Item = EncodedIndexKey> {}

impl<I: Iterator<Item = EncodedIndexKey>> EncodedIndexKeyIterator for I {}

#[derive(Clone)]
pub enum EncodedIndexKey {
	Inline {
		len: u8,
		buf: [u8; 62],
	},
	Heap(Vec<u8>),
}

const _: () = assert!(mem::size_of::<EncodedIndexKey>() == 64);

impl EncodedIndexKey {
	const INLINE_CAP: usize = 62;

	pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
		let vec = bytes.into();
		if vec.len() <= Self::INLINE_CAP {
			let len = vec.len() as u8;
			let mut buf = [0u8; 62];
			buf[..vec.len()].copy_from_slice(&vec);
			EncodedIndexKey::Inline {
				len,
				buf,
			}
		} else {
			EncodedIndexKey::Heap(vec)
		}
	}

	pub fn from_bytes(bytes: &[u8]) -> Self {
		Self::new(bytes.to_vec())
	}

	pub fn as_slice(&self) -> &[u8] {
		match self {
			EncodedIndexKey::Inline {
				len,
				buf,
			} => &buf[..*len as usize],
			EncodedIndexKey::Heap(v) => v.as_slice(),
		}
	}

	pub fn make_mut(&mut self) -> &mut [u8] {
		match self {
			EncodedIndexKey::Inline {
				len,
				buf,
			} => &mut buf[..*len as usize],
			EncodedIndexKey::Heap(v) => v.as_mut_slice(),
		}
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		let byte = index / 8;
		let bit = index % 8;
		(self.as_slice()[byte] & (1 << bit)) != 0
	}

	pub(crate) fn set_valid(&mut self, index: usize, bitvec: bool) {
		let byte = index / 8;
		let bit = index % 8;
		if bitvec {
			self.make_mut()[byte] |= 1 << bit;
		} else {
			self.make_mut()[byte] &= !(1 << bit);
		}
	}
}

impl Deref for EncodedIndexKey {
	type Target = [u8];

	fn deref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl AsRef<[u8]> for EncodedIndexKey {
	fn as_ref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl Borrow<[u8]> for EncodedIndexKey {
	fn borrow(&self) -> &[u8] {
		self.as_slice()
	}
}

impl PartialEq for EncodedIndexKey {
	fn eq(&self, other: &Self) -> bool {
		self.as_slice() == other.as_slice()
	}
}

impl Eq for EncodedIndexKey {}

impl PartialOrd for EncodedIndexKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for EncodedIndexKey {
	fn cmp(&self, other: &Self) -> Ordering {
		self.as_slice().cmp(other.as_slice())
	}
}

impl Hash for EncodedIndexKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_slice().hash(state);
	}
}

impl Serialize for EncodedIndexKey {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.as_slice().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for EncodedIndexKey {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let vec = Vec::<u8>::deserialize(deserializer)?;
		Ok(EncodedIndexKey::new(vec))
	}
}

impl fmt::Debug for EncodedIndexKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "EncodedIndexKey({:02x?})", self.as_slice())
	}
}
