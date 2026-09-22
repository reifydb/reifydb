// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt, mem,
	ops::{Deref, Range},
	sync::Arc,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub struct SharedVec<T> {
	repr: Repr<T>,
}

enum Repr<T> {
	Owned(Vec<T>),
	Shared {
		data: Arc<Vec<T>>,
		offset: usize,
		len: usize,
	},
}

impl<T> SharedVec<T> {
	pub const fn new() -> Self {
		Self {
			repr: Repr::Owned(Vec::new()),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			repr: Repr::Owned(Vec::with_capacity(capacity)),
		}
	}

	pub fn from_vec(vec: Vec<T>) -> Self {
		Self {
			repr: Repr::Owned(vec),
		}
	}

	pub fn frozen(vec: Vec<T>) -> Self {
		let len = vec.len();
		Self {
			repr: Repr::Shared {
				data: Arc::new(vec),
				offset: 0,
				len,
			},
		}
	}

	pub fn len(&self) -> usize {
		match &self.repr {
			Repr::Owned(vec) => vec.len(),
			Repr::Shared {
				len,
				..
			} => *len,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> usize {
		match &self.repr {
			Repr::Owned(vec) => vec.capacity(),
			Repr::Shared {
				data,
				len,
				..
			} => {
				if Arc::strong_count(data) == 1 {
					data.capacity()
				} else {
					*len
				}
			}
		}
	}

	pub fn as_slice(&self) -> &[T] {
		match &self.repr {
			Repr::Owned(vec) => vec.as_slice(),
			Repr::Shared {
				data,
				offset,
				len,
			} => &data[*offset..*offset + *len],
		}
	}

	pub fn is_frozen(&self) -> bool {
		matches!(self.repr, Repr::Shared { .. })
	}

	pub fn freeze(&mut self) {
		if let Repr::Owned(vec) = &mut self.repr {
			let vec = mem::take(vec);
			let len = vec.len();
			self.repr = Repr::Shared {
				data: Arc::new(vec),
				offset: 0,
				len,
			};
		}
	}

	fn clamp(&self, start: usize, end: usize) -> Range<usize> {
		let len = self.len();
		let end = end.min(len);
		let start = start.min(end);
		start..end
	}
}

impl<T: Clone> SharedVec<T> {
	pub fn make_mut(&mut self) -> &mut Vec<T> {
		if self.is_frozen() {
			self.thaw();
		}
		match &mut self.repr {
			Repr::Owned(vec) => vec,
			Repr::Shared {
				..
			} => unreachable!("thaw always leaves the storage owned"),
		}
	}

	pub fn push(&mut self, value: T) {
		self.make_mut().push(value);
	}

	pub fn extend_from_slice(&mut self, other: &[T]) {
		self.make_mut().extend_from_slice(other);
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		let range = self.clamp(start, end);
		match &self.repr {
			Repr::Owned(vec) => Self::frozen(vec[range].to_vec()),
			Repr::Shared {
				data,
				offset,
				..
			} => Self {
				repr: Repr::Shared {
					data: Arc::clone(data),
					offset: offset + range.start,
					len: range.len(),
				},
			},
		}
	}

	fn thaw(&mut self) {
		let repr = mem::replace(&mut self.repr, Repr::Owned(Vec::new()));
		self.repr = match repr {
			Repr::Shared {
				data,
				offset,
				len,
			} => Repr::Owned(match Arc::try_unwrap(data) {
				Ok(mut vec) => {
					vec.truncate(offset + len);
					vec.drain(..offset);
					vec
				}
				Err(data) => data[offset..offset + len].to_vec(),
			}),
			owned => owned,
		};
	}
}

impl<T: Clone> Clone for SharedVec<T> {
	fn clone(&self) -> Self {
		match &self.repr {
			Repr::Owned(vec) => Self::frozen(vec.clone()),
			Repr::Shared {
				data,
				offset,
				len,
			} => Self {
				repr: Repr::Shared {
					data: Arc::clone(data),
					offset: *offset,
					len: *len,
				},
			},
		}
	}
}

impl<T> Default for SharedVec<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> Deref for SharedVec<T> {
	type Target = [T];

	fn deref(&self) -> &[T] {
		self.as_slice()
	}
}

impl<T: PartialEq> PartialEq for SharedVec<T> {
	fn eq(&self, other: &Self) -> bool {
		self.as_slice() == other.as_slice()
	}
}

impl<T: fmt::Debug> fmt::Debug for SharedVec<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Debug::fmt(self.as_slice(), f)
	}
}

impl<T> From<Vec<T>> for SharedVec<T> {
	fn from(vec: Vec<T>) -> Self {
		Self::from_vec(vec)
	}
}

impl<T> FromIterator<T> for SharedVec<T> {
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		Self::from_vec(iter.into_iter().collect())
	}
}

impl<T: Serialize> Serialize for SharedVec<T> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.as_slice().serialize(serializer)
	}
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedVec<T> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		Vec::deserialize(deserializer).map(Self::from_vec)
	}
}
