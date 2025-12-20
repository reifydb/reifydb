// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues,
	value::encoded::EncodedValues,
};

pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}

pub trait SingleVersionCommit {
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()>;
}

pub trait SingleVersionGet {
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;
}

pub trait SingleVersionContains {
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait SingleVersionSet: SingleVersionCommit {
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

pub trait SingleVersionRemove: SingleVersionCommit {
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

pub trait SingleVersionIter: Iterator<Item = SingleVersionValues> + Send {}
impl<T> SingleVersionIter for T where T: Iterator<Item = SingleVersionValues> + Send {}

pub trait SingleVersionRange {
	type Range<'a>: SingleVersionIter
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> crate::Result<Self::Range<'_>>;

	fn prefix(&self, prefix: &EncodedKey) -> crate::Result<Self::Range<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionRangeRev {
	type RangeRev<'a>: SingleVersionIter
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>>;

	fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<Self::RangeRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
