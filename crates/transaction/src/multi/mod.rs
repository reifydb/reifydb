// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Error,
	event::EventBus,
	interface::{
		BoxedMultiVersionIter, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
		MultiVersionTransaction, MultiVersionValues, TransactionId, WithEventBus,
	},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::TransactionStore;

/// Error returned when waiting for watermark times out
#[derive(Debug, Clone)]
pub struct AwaitWatermarkError {
	pub version: CommitVersion,
	pub timeout: Duration,
}

impl std::fmt::Display for AwaitWatermarkError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Timeout waiting for watermark to reach version {} after {:?}", self.version.0, self.timeout)
	}
}

impl std::error::Error for AwaitWatermarkError {}

use crate::{
	multi::{
		pending::PendingWrites,
		transaction::{
			optimistic::{
				CommandTransaction as OptimisticCommandTransaction,
				QueryTransaction as OptimisticQueryTransaction, TransactionOptimistic,
			},
			serializable::{
				CommandTransaction as SerializableCommandTransaction,
				QueryTransaction as SerializableQueryTransaction, TransactionSerializable,
			},
		},
	},
	single::TransactionSingleVersion,
};

pub mod conflict;
pub mod marker;
pub mod optimistic;
pub mod pending;
pub mod serializable;
pub mod transaction;
pub mod types;
pub mod watermark;

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionMultiVersion {
	Optimistic(TransactionOptimistic) = 0,
	Serializable(TransactionSerializable) = 1,
}

impl TransactionMultiVersion {
	pub fn optimistic(store: TransactionStore, single: TransactionSingleVersion, bus: EventBus) -> Self {
		Self::Optimistic(TransactionOptimistic::new(store, single, bus))
	}

	pub fn serializable(store: TransactionStore, single: TransactionSingleVersion, bus: EventBus) -> Self {
		Self::Serializable(TransactionSerializable::new(store, single, bus))
	}

	/// Wait for the watermark to reach the specified version.
	/// Returns Ok(()) if the watermark reaches the version within the timeout,
	/// or Err(AwaitWatermarkError) if the timeout expires.
	pub fn try_wait_for_watermark(
		&self,
		version: CommitVersion,
		timeout: Duration,
	) -> Result<(), AwaitWatermarkError> {
		match self {
			Self::Optimistic(t) => t.tm.try_wait_for_watermark(version, timeout),
			Self::Serializable(t) => t.tm.try_wait_for_watermark(version, timeout),
		}
	}

	/// Get the current version from the transaction manager
	pub fn current_version(&self) -> crate::Result<CommitVersion> {
		match self {
			Self::Optimistic(t) => t.tm.version(),
			Self::Serializable(t) => t.tm.version(),
		}
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	pub fn done_until(&self) -> CommitVersion {
		match self {
			Self::Optimistic(t) => t.tm.done_until(),
			Self::Serializable(t) => t.tm.done_until(),
		}
	}

	/// Returns (query_done_until, command_done_until) for debugging watermark state.
	pub fn watermarks(&self) -> (CommitVersion, CommitVersion) {
		match self {
			Self::Optimistic(t) => t.tm.watermarks(),
			Self::Serializable(t) => t.tm.watermarks(),
		}
	}
}

pub enum StandardQueryTransaction {
	Optimistic(OptimisticQueryTransaction),
	Serializable(SerializableQueryTransaction),
}

pub enum StandardCommandTransaction {
	Optimistic(OptimisticCommandTransaction),
	Serializable(SerializableCommandTransaction),
}

impl WithEventBus for TransactionMultiVersion {
	fn event_bus(&self) -> &EventBus {
		match self {
			TransactionMultiVersion::Optimistic(t) => t.event_bus(),
			TransactionMultiVersion::Serializable(t) => t.event_bus(),
		}
	}
}

impl MultiVersionQueryTransaction for StandardQueryTransaction {
	fn version(&self) -> CommitVersion {
		match self {
			StandardQueryTransaction::Optimistic(q) => q.version(),
			StandardQueryTransaction::Serializable(q) => q.version(),
		}
	}

	fn id(&self) -> TransactionId {
		match self {
			StandardQueryTransaction::Optimistic(q) => q.tm.id(),
			StandardQueryTransaction::Serializable(q) => q.tm.id(),
		}
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => Ok(q.get(key)?),
			StandardQueryTransaction::Serializable(q) => Ok(q.get(key)?),
		}
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => q.contains_key(key),
			StandardQueryTransaction::Serializable(q) => q.contains_key(key),
		}
	}

	fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => {
				let iter = q.range_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter()))
			}
			StandardQueryTransaction::Serializable(q) => {
				let iter = q.range_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter()))
			}
		}
	}

	fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => {
				let iter = q.range_rev_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter()))
			}
			StandardQueryTransaction::Serializable(q) => {
				let iter = q.range_rev_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter()))
			}
		}
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => {
				let iter = q.prefix(prefix)?;
				Ok(Box::new(iter.into_iter()))
			}
			StandardQueryTransaction::Serializable(q) => {
				let iter = q.prefix(prefix)?;
				Ok(Box::new(iter.into_iter()))
			}
		}
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => {
				let iter = q.prefix_rev(prefix)?;
				Ok(Box::new(iter.into_iter()))
			}
			StandardQueryTransaction::Serializable(q) => {
				let iter = q.prefix_rev(prefix)?;
				Ok(Box::new(iter.into_iter()))
			}
		}
	}

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		match self {
			StandardQueryTransaction::Optimistic(q) => {
				q.read_as_of_version_exclusive(version);
				Ok(())
			}
			StandardQueryTransaction::Serializable(q) => {
				q.read_as_of_version_exclusive(version);
				Ok(())
			}
		}
	}
}

impl MultiVersionCommandTransaction for StandardCommandTransaction {
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<(), Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.set(key, values),
			StandardCommandTransaction::Serializable(c) => c.set(key, values),
		}
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.remove(key),
			StandardCommandTransaction::Serializable(c) => c.remove(key),
		}
	}

	fn commit(self) -> Result<CommitVersion, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.commit(),
			StandardCommandTransaction::Serializable(c) => c.commit(),
		}
	}

	fn rollback(self) -> Result<(), Error> {
		// Both transaction types auto-rollback when dropped
		Ok(())
	}
}

impl MultiVersionQueryTransaction for StandardCommandTransaction {
	fn version(&self) -> CommitVersion {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.tm.version(),
			StandardCommandTransaction::Serializable(c) => c.tm.version(),
		}
	}

	fn id(&self) -> TransactionId {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.tm.id(),
			StandardCommandTransaction::Serializable(c) => c.tm.id(),
		}
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				Ok(c.get(key)?.map(|tv| tv.into_multi_version_values()))
			}
			StandardCommandTransaction::Serializable(c) => {
				Ok(c.get(key)?.map(|tv| tv.into_multi_version_values()))
			}
		}
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.contains_key(key),
			StandardCommandTransaction::Serializable(c) => c.contains_key(key),
		}
	}

	fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				let iter = c.range_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
			StandardCommandTransaction::Serializable(c) => {
				let iter = c.range_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
		}
	}

	fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				let iter = c.range_rev_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
			StandardCommandTransaction::Serializable(c) => {
				let iter = c.range_rev_batched(range, batch_size)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
		}
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				let iter = c.prefix(prefix)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
			StandardCommandTransaction::Serializable(c) => {
				let iter = c.prefix(prefix)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
		}
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedMultiVersionIter<'_>, Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				let iter = c.prefix_rev(prefix)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
			StandardCommandTransaction::Serializable(c) => {
				let iter = c.prefix_rev(prefix)?;
				Ok(Box::new(iter.into_iter().map(|tv| tv.into_multi_version_values())))
			}
		}
	}

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		match self {
			StandardCommandTransaction::Optimistic(c) => {
				c.read_as_of_version_exclusive(version);
				Ok(())
			}
			StandardCommandTransaction::Serializable(c) => {
				c.read_as_of_version_exclusive(version);
				Ok(())
			}
		}
	}
}

impl StandardCommandTransaction {
	/// Get access to the pending writes in this transaction
	pub fn pending_writes(&self) -> &PendingWrites {
		match self {
			StandardCommandTransaction::Optimistic(c) => c.pending_writes(),
			StandardCommandTransaction::Serializable(c) => c.pending_writes(),
		}
	}
}

impl MultiVersionTransaction for TransactionMultiVersion {
	type Query = StandardQueryTransaction;
	type Command = StandardCommandTransaction;

	fn begin_query(&self) -> Result<Self::Query, Error> {
		match self {
			TransactionMultiVersion::Optimistic(t) => {
				Ok(StandardQueryTransaction::Optimistic(t.begin_query()?))
			}
			TransactionMultiVersion::Serializable(t) => {
				Ok(StandardQueryTransaction::Serializable(t.begin_query()?))
			}
		}
	}

	fn begin_command(&self) -> Result<Self::Command, Error> {
		match self {
			TransactionMultiVersion::Optimistic(t) => {
				Ok(StandardCommandTransaction::Optimistic(t.begin_command()?))
			}
			TransactionMultiVersion::Serializable(t) => {
				Ok(StandardCommandTransaction::Serializable(t.begin_command()?))
			}
		}
	}
}
