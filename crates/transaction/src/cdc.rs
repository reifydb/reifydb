// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcQueryTransaction, CdcTransaction},
};
use reifydb_store_transaction::{CdcCount, CdcGet, CdcRange, CdcScan, TransactionStore};

#[derive(Clone)]
pub struct TransactionCdc {
	store: TransactionStore,
}

impl TransactionCdc {
	pub fn new(store: TransactionStore) -> Self {
		Self {
			store,
		}
	}
}

impl CdcTransaction for TransactionCdc {
	type Query<'a>
		= StandardCdcQueryTransaction
	where
		Self: 'a;

	fn begin_query(&self) -> Result<Self::Query<'_>> {
		Ok(StandardCdcQueryTransaction::new(self.store.clone()))
	}
}

#[derive(Clone)]
pub struct StandardCdcQueryTransaction {
	store: TransactionStore,
}

impl StandardCdcQueryTransaction {
	pub fn new(store: TransactionStore) -> Self {
		Self {
			store,
		}
	}
}

impl CdcQueryTransaction for StandardCdcQueryTransaction {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		self.store.get(version)
	}

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> Result<Box<dyn Iterator<Item = Cdc> + '_>> {
		Ok(Box::new(self.store.range(start, end)?))
	}

	fn scan(&self) -> Result<Box<dyn Iterator<Item = Cdc> + '_>> {
		Ok(Box::new(self.store.scan()?))
	}

	fn count(&self, version: CommitVersion) -> Result<usize> {
		self.store.count(version)
	}
}
