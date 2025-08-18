// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange, Error, Version,
	hook::Hooks,
	interface::{
		BoxedVersionedIter, GetHooks, UnversionedTransaction,
		Versioned, VersionedCommandTransaction,
		VersionedQueryTransaction, VersionedStorage,
		VersionedTransaction,
	},
	row::EncodedRow,
};

use crate::mvcc::transaction::optimistic::{
	CommandTransaction, Optimistic, QueryTransaction,
};

impl<VS: VersionedStorage, UT: UnversionedTransaction> GetHooks
	for Optimistic<VS, UT>
{
	fn get_hooks(&self) -> &Hooks {
		&self.hooks
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedTransaction
	for Optimistic<VS, UT>
{
	type Query = QueryTransaction<VS, UT>;
	type Command = CommandTransaction<VS, UT>;

	fn begin_query(&self) -> Result<Self::Query, Error> {
		self.begin_query()
	}

	fn begin_command(&self) -> Result<Self::Command, Error> {
		self.begin_command()
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedQueryTransaction
	for QueryTransaction<VS, UT>
{
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> Result<Option<Versioned>, Error> {
		Ok(QueryTransaction::get(self, key)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		}))
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		QueryTransaction::contains_key(self, key)
	}

	fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::scan(self)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::scan_rev(self)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::range(self, range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::range_rev(self, range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::prefix(self, prefix)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = QueryTransaction::prefix_rev(self, prefix)?;
		Ok(Box::new(iter.into_iter()))
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedQueryTransaction
	for CommandTransaction<VS, UT>
{
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> Result<Option<Versioned>, Error> {
		Ok(CommandTransaction::get(self, key)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		}))
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		Ok(CommandTransaction::contains_key(self, key)?)
	}

	fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
		let iter = self.scan()?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
		let iter = self.scan_rev()?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = self.range(range)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = self.range_rev(range)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = self.prefix(prefix)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> Result<BoxedVersionedIter, Error> {
		let iter = self.prefix_rev(prefix)?.map(|tv| Versioned {
			key: tv.key().clone(),
			row: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}
}

impl<VS: VersionedStorage, UT: UnversionedTransaction>
	VersionedCommandTransaction for CommandTransaction<VS, UT>
{
	fn set(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> Result<(), Error> {
		CommandTransaction::set(self, key, row)?;
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		CommandTransaction::remove(self, key)?;
		Ok(())
	}

	fn commit(mut self) -> Result<Version, Error> {
		let version = CommandTransaction::commit(&mut self)?;
		Ok(version)
	}

	fn rollback(mut self) -> Result<(), Error> {
		CommandTransaction::rollback(&mut self)?;
		Ok(())
	}
}
