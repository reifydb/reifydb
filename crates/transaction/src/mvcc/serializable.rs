// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Error,
	event::EventBus,
	interface::{
		BoxedMultiVersionIter, MultiVersionCommandTransaction, MultiVersionQueryTransaction, MultiVersionStore,
		MultiVersionTransaction, MultiVersionValues, SingleVersionTransaction, TransactionId, WithEventBus,
	},
	value::encoded::EncodedValues,
};

use crate::mvcc::transaction::serializable::{CommandTransaction, QueryTransaction, Serializable};

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> WithEventBus for Serializable<MVS, SMVT> {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> MultiVersionTransaction for Serializable<MVS, SMVT> {
	type Query = QueryTransaction<MVS, SMVT>;
	type Command = CommandTransaction<MVS, SMVT>;

	fn begin_query(&self) -> Result<Self::Query, Error> {
		self.begin_query()
	}

	fn begin_command(&self) -> Result<Self::Command, Error> {
		self.begin_command()
	}
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> MultiVersionQueryTransaction
	for QueryTransaction<MVS, SMVT>
{
	fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	fn id(&self) -> TransactionId {
		self.tm.id()
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		Ok(QueryTransaction::get(self, key)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		}))
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		QueryTransaction::contains_key(self, key)
	}

	fn scan(&mut self) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::scan(self)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn scan_rev(&mut self) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::scan_rev(self)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range(&mut self, range: EncodedKeyRange) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::range(self, range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::range_rev(self, range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::prefix(self, prefix)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter, Error> {
		let iter = QueryTransaction::prefix_rev(self, prefix)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		QueryTransaction::read_as_of_version_inclusive(self, version);
		Ok(())
	}
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> MultiVersionQueryTransaction
	for CommandTransaction<MVS, SMVT>
{
	fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	fn id(&self) -> TransactionId {
		self.tm.id()
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		Ok(CommandTransaction::get(self, key)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		}))
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		Ok(CommandTransaction::contains_key(self, key)?)
	}

	fn scan(&mut self) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.scan()?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn scan_rev(&mut self) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.scan_rev()?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn range(&mut self, range: EncodedKeyRange) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.range(range)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.range_rev(range)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.prefix(prefix)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter, Error> {
		let iter = self.prefix_rev(prefix)?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.row().clone(),
			version: tv.version(),
		});

		Ok(Box::new(iter))
	}

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		self.read_as_of_version_inclusive(version)?;
		Ok(())
	}
}

impl<MVS: MultiVersionStore, SMVT: SingleVersionTransaction> MultiVersionCommandTransaction
	for CommandTransaction<MVS, SMVT>
{
	fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> Result<(), Error> {
		CommandTransaction::set(self, key, row)?;
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		CommandTransaction::remove(self, key)?;
		Ok(())
	}

	fn commit(mut self) -> Result<CommitVersion, Error> {
		let version = CommandTransaction::commit(&mut self)?;
		Ok(version)
	}

	fn rollback(mut self) -> Result<(), Error> {
		CommandTransaction::rollback(&mut self)?;
		Ok(())
	}
}
