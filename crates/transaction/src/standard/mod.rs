// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::{MultiVersionBatch, MultiVersionValues},
};
use reifydb_type::Result;

use crate::{
	TransactionId,
	single::svl::read::SvlQueryTransaction,
	standard::{command::StandardCommandTransaction, query::StandardQueryTransaction},
};

pub mod catalog;
pub mod command;
pub mod query;

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
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key),
			Self::Query(txn) => txn.get(key),
		}
	}

	/// Check if a key exists (async method)
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key),
			Self::Query(txn) => txn.contains_key(key),
		}
	}

	/// Get a prefix batch (async method)
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
		}
	}

	/// Get a reverse prefix batch (async method)
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
		}
	}

	/// Read as of version exclusive (async method)
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		match self {
			StandardTransaction::Command(txn) => txn.read_as_of_version_exclusive(version),
			StandardTransaction::Query(txn) => txn.read_as_of_version_exclusive(version),
		}
	}

	/// Create a streaming iterator for forward range queries.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		match self {
			StandardTransaction::Command(txn) => txn.range(range, batch_size),
			StandardTransaction::Query(txn) => Ok(txn.range(range, batch_size)),
		}
	}

	/// Create a streaming iterator for reverse range queries.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		match self {
			StandardTransaction::Command(txn) => txn.range_rev(range, batch_size),
			StandardTransaction::Query(txn) => Ok(txn.range_rev(range, batch_size)),
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
pub trait IntoStandardTransaction: Send {
	fn into_standard_transaction(&mut self) -> StandardTransaction<'_>;

	/// Get a value by key (async method)
	fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>>
	where
		Self: Sized,
	{
		self.into_standard_transaction().get(key)
	}

	/// Check if a key exists (async method)
	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool>
	where
		Self: Sized,
	{
		self.into_standard_transaction().contains_key(key)
	}

	/// Get a prefix batch (async method)
	fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().prefix(prefix)
	}

	/// Get a reverse prefix batch (async method)
	fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch>
	where
		Self: Sized,
	{
		self.into_standard_transaction().prefix_rev(prefix)
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
	pub fn begin_single_query<'b, I>(&self, keys: I) -> Result<SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			StandardTransaction::Command(txn) => txn.begin_single_query(keys),
			StandardTransaction::Query(txn) => txn.begin_single_query(keys),
		}
	}
}
