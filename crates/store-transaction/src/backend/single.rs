// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, delta::Delta, value::encoded::EncodedValues};

use crate::{
	backend::{
		memory,
		result::{SingleVersionGetResult, SingleVersionIterResult},
		sqlite,
	},
	memory::MemoryBackend,
	sqlite::SqliteBackend,
};

#[repr(u8)]
#[derive(Clone)]
pub enum BackendSingle {
	Memory(MemoryBackend) = 0,
	Sqlite(SqliteBackend) = 1,
	// Other(Box<dyn >) = 254,
}

impl BackendSingleVersionCommit for BackendSingle {
	#[inline]
	fn commit(&self, deltas: CowVec<Delta>) -> reifydb_type::Result<()> {
		match self {
			BackendSingle::Memory(backend) => backend.commit(deltas),
			BackendSingle::Sqlite(backend) => backend.commit(deltas),
		}
	}
}

impl BackendSingleVersionGet for BackendSingle {
	#[inline]
	fn get(&self, key: &EncodedKey) -> reifydb_type::Result<SingleVersionGetResult> {
		match self {
			BackendSingle::Memory(backend) => backend.get(key),
			BackendSingle::Sqlite(backend) => backend.get(key),
		}
	}
}

impl BackendSingleVersionContains for BackendSingle {
	#[inline]
	fn contains(&self, key: &EncodedKey) -> reifydb_type::Result<bool> {
		match self {
			BackendSingle::Memory(backend) => backend.contains(key),
			BackendSingle::Sqlite(backend) => backend.contains(key),
		}
	}
}

impl BackendSingleVersionSet for BackendSingle {}

impl BackendSingleVersionRemove for BackendSingle {}

pub enum BackendSingleRangeIter<'a> {
	Memory(memory::SingleVersionRangeIter, PhantomData<&'a ()>),
	Sqlite(sqlite::SingleVersionRangeIter),
}

impl<'a> Iterator for BackendSingleRangeIter<'a> {
	type Item = SingleVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleRangeIter::Memory(iter, _) => iter.next(),
			BackendSingleRangeIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendSingleVersionRange for BackendSingle {
	type Range<'a> = BackendSingleRangeIter<'a>;

	#[inline]
	fn range(&self, range: EncodedKeyRange) -> reifydb_type::Result<Self::Range<'_>> {
		match self {
			BackendSingle::Memory(backend) => {
				backend.range(range).map(|iter| BackendSingleRangeIter::Memory(iter, PhantomData))
			}
			BackendSingle::Sqlite(backend) => backend.range(range).map(BackendSingleRangeIter::Sqlite),
		}
	}
}

pub enum BackendSingleRangeIterRev<'a> {
	Memory(memory::SingleVersionRangeRevIter, PhantomData<&'a ()>),
	Sqlite(sqlite::SingleVersionRangeRevIter),
}

impl<'a> Iterator for BackendSingleRangeIterRev<'a> {
	type Item = SingleVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleRangeIterRev::Memory(iter, _) => iter.next(),
			BackendSingleRangeIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendSingleVersionRangeRev for BackendSingle {
	type RangeRev<'a> = BackendSingleRangeIterRev<'a>;

	#[inline]
	fn range_rev(&self, range: EncodedKeyRange) -> reifydb_type::Result<Self::RangeRev<'_>> {
		match self {
			BackendSingle::Memory(backend) => backend
				.range_rev(range)
				.map(|iter| BackendSingleRangeIterRev::Memory(iter, PhantomData)),
			BackendSingle::Sqlite(backend) => {
				backend.range_rev(range).map(BackendSingleRangeIterRev::Sqlite)
			}
		}
	}
}

impl BackendSingleVersion for BackendSingle {}

pub trait BackendSingleVersion:
	Send
	+ Sync
	+ Clone
	+ BackendSingleVersionCommit
	+ BackendSingleVersionGet
	+ BackendSingleVersionContains
	+ BackendSingleVersionSet
	+ BackendSingleVersionRemove
	+ BackendSingleVersionRange
	+ BackendSingleVersionRangeRev
	+ 'static
{
}

pub trait BackendSingleVersionCommit {
	fn commit(&self, deltas: CowVec<Delta>) -> crate::Result<()>;
}

pub trait BackendSingleVersionGet {
	fn get(&self, key: &EncodedKey) -> crate::Result<SingleVersionGetResult>;
}

pub trait BackendSingleVersionContains {
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait BackendSingleVersionSet: BackendSingleVersionCommit {
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				values: values.clone(),
			}]),
		)
	}
}

pub trait BackendSingleVersionRemove: BackendSingleVersionCommit {
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

pub trait BackendSingleVersionIter: Iterator<Item = SingleVersionIterResult> + Send {}
impl<T> BackendSingleVersionIter for T where T: Iterator<Item = SingleVersionIterResult> + Send {}

pub trait BackendSingleVersionRange {
	type Range<'a>: BackendSingleVersionIter
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> crate::Result<Self::Range<'_>>;

	fn prefix(&self, prefix: &EncodedKey) -> crate::Result<Self::Range<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait BackendSingleVersionRangeRev {
	type RangeRev<'a>: BackendSingleVersionIter
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>>;

	fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<Self::RangeRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
