// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::pin::Pin;

use futures_util::Stream;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, interface::MultiVersionValues};
use reifydb_store_transaction::MultiVersionBatch;
use reifydb_type::Result;

use crate::TransactionId;

mod catalog;
mod command;
mod query;

pub use command::StandardCommandTransaction;
pub use query::StandardQueryTransaction;

/// An enum that can hold either a command or query transaction for flexible
/// execution
pub enum StandardTransaction<'a> {
	Command(&'a mut StandardCommandTransaction),
	Query(&'a mut StandardQueryTransaction),
}

impl<'a> StandardTransaction<'a> {
	/// Get the transaction version
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Command(txn) => txn.version(),
			Self::Query(txn) => txn.version(),
		}
	}

	/// Get the transaction ID
	pub fn id(&self) -> TransactionId {
		match self {
			Self::Command(txn) => txn.id(),
			Self::Query(txn) => txn.id(),
		}
	}

	/// Get a value by key (async method)
	pub async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key).await,
			Self::Query(txn) => txn.get(key).await,
		}
	}

	/// Check if a key exists (async method)
	pub async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key).await,
			Self::Query(txn) => txn.contains_key(key).await,
		}
	}

	/// Get a range batch (async method)
	pub async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_batch(range, batch_size).await,
		}
	}

	/// Get a reverse range batch (async method)
	pub async fn range_rev_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_rev_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_rev_batch(range, batch_size).await,
		}
	}

	/// Get a prefix batch (async method)
	pub async fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix).await,
			Self::Query(txn) => txn.prefix(prefix).await,
		}
	}

	/// Get a reverse prefix batch (async method)
	pub async fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix).await,
			Self::Query(txn) => txn.prefix_rev(prefix).await,
		}
	}

	/// Read as of version exclusive (async method)
	pub async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		match self {
			StandardTransaction::Command(txn) => txn.read_as_of_version_exclusive(version).await,
			StandardTransaction::Query(txn) => txn.read_as_of_version_exclusive(version).await,
		}
	}

	/// Create a streaming iterator for forward range queries.
	pub fn range_stream(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Pin<Box<dyn Stream<Item = Result<MultiVersionValues>> + Send + '_>>> {
		match self {
			StandardTransaction::Command(txn) => txn.range_stream(range, batch_size),
			StandardTransaction::Query(txn) => Ok(txn.range_stream(range, batch_size)),
		}
	}

	/// Create a streaming iterator for reverse range queries.
	pub fn range_rev_stream(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Pin<Box<dyn Stream<Item = Result<MultiVersionValues>> + Send + '_>>> {
		match self {
			StandardTransaction::Command(txn) => txn.range_rev_stream(range, batch_size),
			StandardTransaction::Query(txn) => Ok(txn.range_rev_stream(range, batch_size)),
		}
	}
}

impl<'a> From<&'a mut StandardCommandTransaction> for StandardTransaction<'a> {
	fn from(txn: &'a mut StandardCommandTransaction) -> Self {
		Self::Command(txn)
	}
}

impl<'a> From<&'a mut StandardQueryTransaction> for StandardTransaction<'a> {
	fn from(txn: &'a mut StandardQueryTransaction) -> Self {
		Self::Query(txn)
	}
}

/// Trait for types that can be converted into a StandardTransaction.
/// This allows functions to accept either StandardCommandTransaction or
/// StandardQueryTransaction directly without requiring manual wrapping.
#[allow(async_fn_in_trait)]
pub trait IntoStandardTransaction: Send {
	fn into_standard_transaction(&mut self) -> StandardTransaction<'_>;

	/// Get a value by key (async method)
	async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>>
	where
		Self: Sized,
	{
		self.into_standard_transaction().get(key).await
	}

	/// Check if a key exists (async method)
	async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool>
	where
		Self: Sized,
	{
		self.into_standard_transaction().contains_key(key).await
	}

	/// Get a range batch (async method)
	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().range_batch(range, batch_size).await
	}

	/// Get a range batch with default batch size (1000)
	async fn range(&mut self, range: EncodedKeyRange) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.range_batch(range, 1000).await
	}

	/// Get a reverse range batch (async method)
	async fn range_rev_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().range_rev_batch(range, batch_size).await
	}

	/// Get a prefix batch (async method)
	async fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().prefix(prefix).await
	}

	/// Get a reverse prefix batch (async method)
	async fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().prefix_rev(prefix).await
	}
}

impl IntoStandardTransaction for StandardCommandTransaction {
	fn into_standard_transaction(&mut self) -> StandardTransaction<'_> {
		StandardTransaction::Command(self)
	}
}

impl IntoStandardTransaction for StandardQueryTransaction {
	fn into_standard_transaction(&mut self) -> StandardTransaction<'_> {
		StandardTransaction::Query(self)
	}
}

impl IntoStandardTransaction for StandardTransaction<'_> {
	fn into_standard_transaction(&mut self) -> StandardTransaction<'_> {
		match self {
			StandardTransaction::Command(cmd) => StandardTransaction::Command(cmd),
			StandardTransaction::Query(qry) => StandardTransaction::Query(qry),
		}
	}
}

impl<'a> StandardTransaction<'a> {
	/// Extract the underlying StandardCommandTransaction, panics if this is
	/// a Query transaction
	pub fn command(self) -> &'a mut StandardCommandTransaction {
		match self {
			Self::Command(txn) => txn,
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Extract the underlying StandardQueryTransaction, panics if this is a
	/// Command transaction
	pub fn query(self) -> &'a mut StandardQueryTransaction {
		match self {
			Self::Query(txn) => txn,
			Self::Command(_) => panic!("Expected Query transaction but found Command transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// StandardCommandTransaction, panics if this is a Query transaction
	pub fn command_mut(&mut self) -> &mut StandardCommandTransaction {
		match self {
			Self::Command(txn) => txn,
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Get a mutable reference to the underlying StandardQueryTransaction,
	/// panics if this is a Command transaction
	pub fn query_mut(&mut self) -> &mut StandardQueryTransaction {
		match self {
			Self::Query(txn) => txn,
			Self::Command(_) => panic!("Expected Query transaction but found Command transaction"),
		}
	}

	/// Begin a single-version query transaction for specific keys
	pub async fn begin_single_query<'b, I>(&self, keys: I) -> Result<crate::single::SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey> + Send,
	{
		match self {
			StandardTransaction::Command(txn) => txn.begin_single_query(keys).await,
			StandardTransaction::Query(txn) => txn.begin_single_query(keys).await,
		}
	}

	/// Begin a CDC query transaction
	pub async fn begin_cdc_query(&self) -> Result<crate::cdc::StandardCdcQueryTransaction> {
		match self {
			StandardTransaction::Command(txn) => txn.begin_cdc_query().await,
			StandardTransaction::Query(txn) => txn.begin_cdc_query().await,
		}
	}
}
