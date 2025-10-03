// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcQueryTransaction, CdcTransaction},
};
use reifydb_store_transaction::CdcStore;

#[derive(Clone)]
pub struct StandardCdcTransaction<S: CdcStore> {
	storage: S,
}

impl<S: CdcStore> StandardCdcTransaction<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}
}

impl<S: CdcStore> CdcTransaction for StandardCdcTransaction<S> {
	type Query<'a>
		= StandardCdcQueryTransaction<S>
	where
		Self: 'a;

	fn begin_query(&self) -> Result<Self::Query<'_>> {
		Ok(StandardCdcQueryTransaction::new(self.storage.clone()))
	}
}

#[derive(Clone)]
pub struct StandardCdcQueryTransaction<S: CdcStore> {
	storage: S,
}

impl<S: CdcStore> StandardCdcQueryTransaction<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}
}

impl<S: CdcStore> CdcQueryTransaction for StandardCdcQueryTransaction<S> {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		self.storage.get(version)
	}

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> Result<Box<dyn Iterator<Item = Cdc> + '_>> {
		Ok(Box::new(self.storage.range(start, end)?))
	}

	fn scan(&self) -> Result<Box<dyn Iterator<Item = Cdc> + '_>> {
		Ok(Box::new(self.storage.scan()?))
	}

	fn count(&self, version: CommitVersion) -> Result<usize> {
		self.storage.count(version)
	}
}
