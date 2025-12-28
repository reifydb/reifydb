// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Error,
	event::EventBus,
	interface::{
		CommandTransaction as CommandTransactionInterface, MultiVersionBatch, MultiVersionTransaction,
		MultiVersionValues, QueryTransaction as QueryTransactionInterface, TransactionId, WithEventBus,
	},
	value::encoded::EncodedValues,
};

use crate::multi::transaction::{CommandTransaction, QueryTransaction, TransactionMulti};

impl WithEventBus for TransactionMulti {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

#[async_trait]
impl MultiVersionTransaction for TransactionMulti {
	type Query = QueryTransaction;
	type Command = CommandTransaction;

	async fn begin_query(&self) -> Result<Self::Query, Error> {
		TransactionMulti::begin_query(self).await
	}

	async fn begin_command(&self) -> Result<Self::Command, Error> {
		TransactionMulti::begin_command(self).await
	}
}

use std::ops::Bound;

use reifydb_core::interface::{Cdc, CdcBatch, SingleVersionQueryTransaction, SingleVersionValues};

/// Stub type for SingleVersionQuery - not available at this layer
pub struct StubSingleVersionQuery;

#[async_trait]
impl SingleVersionQueryTransaction for StubSingleVersionQuery {
	async fn get(&mut self, _key: &EncodedKey) -> reifydb_core::Result<Option<SingleVersionValues>> {
		unimplemented!("SingleVersionQueryTransaction not available at transaction layer")
	}

	async fn contains_key(&mut self, _key: &EncodedKey) -> reifydb_core::Result<bool> {
		unimplemented!("SingleVersionQueryTransaction not available at transaction layer")
	}
}

/// Stub type for CdcQuery - not available at this layer
#[derive(Clone)]
pub struct StubCdcQuery;

#[async_trait]
impl reifydb_core::interface::CdcQueryTransaction for StubCdcQuery {
	async fn get(&self, _version: CommitVersion) -> reifydb_core::Result<Option<Cdc>> {
		unimplemented!("CdcQueryTransaction not available at transaction layer")
	}

	async fn range_batch(
		&self,
		_start: Bound<CommitVersion>,
		_end: Bound<CommitVersion>,
		_batch_size: u64,
	) -> reifydb_core::Result<CdcBatch> {
		unimplemented!("CdcQueryTransaction not available at transaction layer")
	}

	async fn count(&self, _version: CommitVersion) -> reifydb_core::Result<usize> {
		unimplemented!("CdcQueryTransaction not available at transaction layer")
	}
}

/// Stub type for SingleVersionCommand - not available at this layer
pub struct StubSingleVersionCommand;

#[async_trait]
impl SingleVersionQueryTransaction for StubSingleVersionCommand {
	async fn get(&mut self, _key: &EncodedKey) -> reifydb_core::Result<Option<SingleVersionValues>> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}

	async fn contains_key(&mut self, _key: &EncodedKey) -> reifydb_core::Result<bool> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}
}

#[async_trait]
impl reifydb_core::interface::SingleVersionCommandTransaction for StubSingleVersionCommand {
	fn set(&mut self, _key: &EncodedKey, _values: EncodedValues) -> reifydb_core::Result<()> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}

	async fn remove(&mut self, _key: &EncodedKey) -> reifydb_core::Result<()> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}

	async fn commit(&mut self) -> reifydb_core::Result<()> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}

	async fn rollback(&mut self) -> reifydb_core::Result<()> {
		unimplemented!("SingleVersionCommandTransaction not available at transaction layer")
	}
}

#[async_trait]
impl QueryTransactionInterface for QueryTransaction {
	type SingleVersionQuery<'a> = StubSingleVersionQuery;
	type CdcQuery<'a> = StubCdcQuery;

	fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	fn id(&self) -> TransactionId {
		self.tm.id()
	}

	async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		Ok(QueryTransaction::get(self, key).await?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.values().clone(),
			version: tv.version(),
		}))
	}

	async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		QueryTransaction::contains_key(self, key).await
	}

	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch, Error> {
		let batch = QueryTransaction::range_batch(self, range, batch_size).await?;
		Ok(MultiVersionBatch {
			items: batch
				.items
				.into_iter()
				.map(|mv| MultiVersionValues {
					key: mv.key,
					values: mv.values,
					version: mv.version,
				})
				.collect(),
			has_more: batch.has_more,
		})
	}

	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<MultiVersionBatch, Error> {
		let batch = QueryTransaction::range_rev_batch(self, range, batch_size).await?;
		Ok(MultiVersionBatch {
			items: batch
				.items
				.into_iter()
				.map(|mv| MultiVersionValues {
					key: mv.key,
					values: mv.values,
					version: mv.version,
				})
				.collect(),
			has_more: batch.has_more,
		})
	}

	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		QueryTransaction::read_as_of_version_exclusive(self, version);
		Ok(())
	}

	async fn begin_single_query<'a, I>(&self, _keys: I) -> Result<Self::SingleVersionQuery<'_>, Error>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		unimplemented!("begin_single_query not available at transaction layer")
	}

	async fn begin_cdc_query(&self) -> Result<Self::CdcQuery<'_>, Error> {
		unimplemented!("begin_cdc_query not available at transaction layer")
	}
}

#[async_trait]
impl QueryTransactionInterface for CommandTransaction {
	type SingleVersionQuery<'a> = StubSingleVersionQuery;
	type CdcQuery<'a> = StubCdcQuery;
	fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	fn id(&self) -> TransactionId {
		self.tm.id()
	}

	async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		Ok(CommandTransaction::get(self, key).await?.map(|tv| MultiVersionValues {
			key: tv.key().clone(),
			values: tv.values().clone(),
			version: tv.version(),
		}))
	}

	async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		CommandTransaction::contains_key(self, key).await
	}

	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch, Error> {
		let batch = CommandTransaction::range_batch(self, range, batch_size).await?;
		Ok(MultiVersionBatch {
			items: batch.items,
			has_more: batch.has_more,
		})
	}

	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<MultiVersionBatch, Error> {
		let batch = CommandTransaction::range_rev_batch(self, range, batch_size).await?;
		Ok(MultiVersionBatch {
			items: batch.items,
			has_more: batch.has_more,
		})
	}

	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		CommandTransaction::read_as_of_version_exclusive(self, version);
		Ok(())
	}

	async fn begin_single_query<'a, I>(&self, _keys: I) -> Result<Self::SingleVersionQuery<'_>, Error>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		unimplemented!("begin_single_query not available at transaction layer")
	}

	async fn begin_cdc_query(&self) -> Result<Self::CdcQuery<'_>, Error> {
		unimplemented!("begin_cdc_query not available at transaction layer")
	}
}

#[async_trait]
impl CommandTransactionInterface for CommandTransaction {
	type SingleVersionCommand<'a> = StubSingleVersionCommand;

	async fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<(), Error> {
		CommandTransaction::set(self, key, values)?;
		Ok(())
	}

	async fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		CommandTransaction::remove(self, key)?;
		Ok(())
	}

	async fn commit(&mut self) -> Result<CommitVersion, Error> {
		let version = CommandTransaction::commit(self).await?;
		Ok(version)
	}

	async fn rollback(&mut self) -> Result<(), Error> {
		CommandTransaction::rollback(self)?;
		Ok(())
	}

	async fn begin_single_command<'a, I>(&self, _keys: I) -> Result<Self::SingleVersionCommand<'_>, Error>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		unimplemented!("begin_single_command not available at transaction layer")
	}
}
