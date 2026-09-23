// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, ops::Deref, sync::Arc, vec};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CowVec<T>
where
	T: Clone + PartialEq,
{
	inner: Arc<Vec<T>>,
}

impl<T> CowVec<T>
where
	T: Clone + PartialEq,
{
	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.is_empty()
	}
}

#[macro_export]
macro_rules! cow_vec {
    () => {
        $crate::util::cowvec::CowVec::new(Vec::new())
    };
    ($($elem:expr),+ $(,)?) => {
        $crate::util::cowvec::CowVec::new(vec![$($elem),+])
    };
}

impl<T> Default for CowVec<T>
where
	T: Clone + PartialEq,
{
	fn default() -> Self {
		Self {
			inner: Arc::new(Vec::new()),
		}
	}
}

impl<T: Clone + PartialEq> PartialEq<[T]> for &CowVec<T> {
	fn eq(&self, other: &[T]) -> bool {
		self.inner.as_slice() == other
	}
}

impl<T: Clone + PartialEq> PartialEq<[T]> for CowVec<T> {
	fn eq(&self, other: &[T]) -> bool {
		self.inner.as_slice() == other
	}
}

impl<T: Clone + PartialEq> PartialEq<CowVec<T>> for [T] {
	fn eq(&self, other: &CowVec<T>) -> bool {
		self == other.inner.as_slice()
	}
}

impl<T: Clone + PartialEq> Clone for CowVec<T> {
	fn clone(&self) -> Self {
		CowVec {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Clone + PartialEq> CowVec<T> {
	pub fn new(vec: Vec<T>) -> Self {
		CowVec {
			inner: Arc::new(vec),
		}
	}

	pub fn into_inner(self) -> Vec<T> {
		match Arc::try_unwrap(self.inner) {
			Ok(vec) => vec,
			Err(arc) => (*arc).clone(),
		}
	}

	pub fn as_slice(&self) -> &[T] {
		&self.inner
	}

	pub fn make_mut(&mut self) -> &mut Vec<T> {
		Arc::make_mut(&mut self.inner)
	}
}

impl<T: Clone + PartialEq> IntoIterator for CowVec<T> {
	type Item = T;
	type IntoIter = vec::IntoIter<T>;

	fn into_iter(self) -> Self::IntoIter {
		match Arc::try_unwrap(self.inner) {
			Ok(vec) => vec.into_iter(),
			Err(arc) => (*arc).clone().into_iter(),
		}
	}
}

impl<T: Clone + PartialEq> Deref for CowVec<T> {
	type Target = [T];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}

impl<T: Clone + PartialEq> Borrow<[T]> for CowVec<T> {
	fn borrow(&self) -> &[T] {
		self.as_slice()
	}
}

impl<T> Serialize for CowVec<T>
where
	T: Clone + PartialEq + Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.inner.serialize(serializer)
	}
}

impl<'de, T> Deserialize<'de> for CowVec<T>
where
	T: Clone + PartialEq + Deserialize<'de>,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let vec = Vec::<T>::deserialize(deserializer)?;
		Ok(CowVec {
			inner: Arc::new(vec),
		})
	}
}
