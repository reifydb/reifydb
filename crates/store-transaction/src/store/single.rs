// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	interface::{
		SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionInsert, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionRemove, SingleVersionScan, SingleVersionScanRev,
		SingleVersionStore, SingleVersionValues,
	},
};

use super::StandardTransactionStore;

pub trait SingleVersionIter: Iterator<Item = SingleVersionValues> + Send {}
impl<T> SingleVersionIter for T where T: Iterator<Item = SingleVersionValues> + Send {}

impl SingleVersionGet for StandardTransactionStore {
	fn get(&self, _key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		todo!("Implement single version get")
	}
}

impl SingleVersionContains for StandardTransactionStore {
	fn contains(&self, _key: &EncodedKey) -> crate::Result<bool> {
		todo!("Implement single version contains")
	}
}

impl SingleVersionCommit for StandardTransactionStore {
	fn commit(&mut self, _deltas: CowVec<Delta>) -> crate::Result<()> {
		todo!("Implement single version commit")
	}
}

impl SingleVersionInsert for StandardTransactionStore {}
impl SingleVersionRemove for StandardTransactionStore {}

impl SingleVersionScan for StandardTransactionStore {
	type ScanIter<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn scan(&self) -> crate::Result<Self::ScanIter<'_>> {
		todo!("Implement single version scan")
	}
}

impl SingleVersionScanRev for StandardTransactionStore {
	type ScanIterRev<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn scan_rev(&self) -> crate::Result<Self::ScanIterRev<'_>> {
		todo!("Implement single version reverse scan")
	}
}

impl SingleVersionRange for StandardTransactionStore {
	type Range<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn range(&self, _range: EncodedKeyRange) -> crate::Result<Self::Range<'_>> {
		todo!("Implement single version range")
	}
}

impl SingleVersionRangeRev for StandardTransactionStore {
	type RangeRev<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn range_rev(&self, _range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>> {
		todo!("Implement single version reverse range")
	}
}

impl SingleVersionStore for StandardTransactionStore {}
