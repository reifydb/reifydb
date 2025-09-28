// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{MultiVersionRow, TransactionId, WithEventBus},
	value::row::EncodedRow,
};

pub type BoxedMultiVersionIter<'a> = Box<dyn Iterator<Item = MultiVersionRow> + Send + 'a>;

pub trait MultiVersionTransaction: WithEventBus + Send + Sync + Clone + 'static {
	type Query: MultiVersionQueryTransaction;
	type Command: MultiVersionCommandTransaction;

	fn begin_query(&self) -> crate::Result<Self::Query>;

	fn begin_command(&self) -> crate::Result<Self::Command>;

	fn with_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Query) -> crate::Result<R>,
	{
		let mut tx = self.begin_query()?;
		f(&mut tx)
	}

	fn with_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Command) -> crate::Result<R>,
	{
		let mut tx = self.begin_command()?;
		let result = f(&mut tx)?;
		let _version = tx.commit()?;
		Ok(result)
	}
}

pub trait MultiVersionQueryTransaction {
	fn version(&self) -> CommitVersion;

	fn id(&self) -> TransactionId;

	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionRow>>;

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

	fn scan(&mut self) -> crate::Result<BoxedMultiVersionIter>;

	fn scan_rev(&mut self) -> crate::Result<BoxedMultiVersionIter>;

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter>;

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter>;

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter>;

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter>;
}

pub trait MultiVersionCommandTransaction: MultiVersionQueryTransaction {
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	fn commit(self) -> crate::Result<crate::CommitVersion>;

	fn rollback(self) -> crate::Result<()>;

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()>;

	fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.read_as_of_version_exclusive(version + 1)
	}
}
