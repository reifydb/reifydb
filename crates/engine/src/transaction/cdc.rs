// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_core::{
	CommitVersion, Result,
	interface::{
		CdcEvent, CdcQueryTransaction, CdcStorage, CdcTransaction,
	},
};

#[derive(Clone)]
pub struct StandardCdcTransaction<S: CdcStorage> {
	storage: S,
}

impl<S: CdcStorage> StandardCdcTransaction<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}
}

impl<S: CdcStorage> CdcTransaction for StandardCdcTransaction<S> {
	type Query<'a>
		= StandardCdcQueryTransaction<S>
	where
		Self: 'a;

	fn begin_query(&self) -> Result<Self::Query<'_>> {
		Ok(StandardCdcQueryTransaction::new(self.storage.clone()))
	}
}

/// CDC transaction wrapper for storage that implements CdcQuery
#[derive(Clone)]
pub struct StandardCdcQueryTransaction<S: CdcStorage> {
	storage: S,
}

impl<S: CdcStorage> StandardCdcQueryTransaction<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}
}

impl<S: CdcStorage> CdcQueryTransaction for StandardCdcQueryTransaction<S> {
	fn get(&self, version: CommitVersion) -> Result<Vec<CdcEvent>> {
		self.storage.get(version)
	}

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>> {
		Ok(Box::new(self.storage.range(start, end)?))
	}

	fn scan(&self) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>> {
		Ok(Box::new(self.storage.scan()?))
	}

	fn count(&self, version: CommitVersion) -> Result<usize> {
		self.storage.count(version)
	}
}
