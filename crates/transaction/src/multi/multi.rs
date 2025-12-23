// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Error,
	event::EventBus,
	interface::{
		MultiVersionBatch, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
		MultiVersionTransaction, MultiVersionValues, TransactionId, WithEventBus,
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

#[async_trait]
impl MultiVersionQueryTransaction for QueryTransaction {
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
}

#[async_trait]
impl MultiVersionQueryTransaction for CommandTransaction {
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
}

#[async_trait]
impl MultiVersionCommandTransaction for CommandTransaction {
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
}
