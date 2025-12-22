// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, TransactionId,
	interface::{MultiVersionBatch, MultiVersionQueryTransaction, MultiVersionValues},
};

mod catalog;
mod command;
#[allow(dead_code)]
pub(crate) mod operation;
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
			Self::Command(txn) => MultiVersionQueryTransaction::version(*txn),
			Self::Query(txn) => MultiVersionQueryTransaction::version(*txn),
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
	pub async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key).await,
			Self::Query(txn) => txn.get(key).await,
		}
	}

	/// Check if a key exists (async method)
	pub async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key).await,
			Self::Query(txn) => txn.contains_key(key).await,
		}
	}

	/// Get a range batch (async method)
	pub async fn range_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_batch(range, batch_size).await,
		}
	}

	/// Get a reverse range batch (async method)
	pub async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_rev_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_rev_batch(range, batch_size).await,
		}
	}

	/// Get a prefix batch (async method)
	pub async fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix).await,
			Self::Query(txn) => txn.prefix(prefix).await,
		}
	}

	/// Get a reverse prefix batch (async method)
	pub async fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix).await,
			Self::Query(txn) => txn.prefix_rev(prefix).await,
		}
	}

	/// Read as of version exclusive (async method)
	pub async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		match self {
			StandardTransaction::Command(txn) => txn.read_as_of_version_exclusive(version).await,
			StandardTransaction::Query(txn) => txn.read_as_of_version_exclusive(version).await,
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

	pub fn catalog(&self) -> &MaterializedCatalog {
		match self {
			StandardTransaction::Command(txn) => &txn.catalog,
			StandardTransaction::Query(txn) => &txn.catalog,
		}
	}
}

use async_trait::async_trait;
use reifydb_core::interface::QueryTransaction;

// StandardTransaction already has MultiVersionQueryTransaction methods defined above,
// but we need the trait implementation for trait bounds
#[async_trait]
impl<'a> MultiVersionQueryTransaction for StandardTransaction<'a> {
	fn version(&self) -> CommitVersion {
		match self {
			Self::Command(txn) => MultiVersionQueryTransaction::version(*txn),
			Self::Query(txn) => MultiVersionQueryTransaction::version(*txn),
		}
	}

	fn id(&self) -> TransactionId {
		match self {
			Self::Command(txn) => txn.id(),
			Self::Query(txn) => txn.id(),
		}
	}

	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key).await,
			Self::Query(txn) => txn.get(key).await,
		}
	}

	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key).await,
			Self::Query(txn) => txn.contains_key(key).await,
		}
	}

	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_batch(range, batch_size).await,
		}
	}

	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.range_rev_batch(range, batch_size).await,
			Self::Query(txn) => txn.range_rev_batch(range, batch_size).await,
		}
	}

	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		match self {
			StandardTransaction::Command(txn) => txn.read_as_of_version_exclusive(version).await,
			StandardTransaction::Query(txn) => txn.read_as_of_version_exclusive(version).await,
		}
	}
}

#[async_trait]
impl<'a> QueryTransaction for StandardTransaction<'a> {
	type SingleVersionQuery<'b>
		= <StandardQueryTransaction as QueryTransaction>::SingleVersionQuery<'b>
	where
		Self: 'b;
	type CdcQuery<'b>
		= <StandardQueryTransaction as QueryTransaction>::CdcQuery<'b>
	where
		Self: 'b;

	async fn begin_single_query<'c, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'c EncodedKey> + Send,
	{
		match self {
			StandardTransaction::Command(txn) => txn.begin_single_query(keys).await,
			StandardTransaction::Query(txn) => txn.begin_single_query(keys).await,
		}
	}

	async fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		match self {
			StandardTransaction::Command(txn) => txn.begin_cdc_query().await,
			StandardTransaction::Query(txn) => txn.begin_cdc_query().await,
		}
	}
}
