// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	interface::store::{MultiVersionBatch, MultiVersionValues},
};
use reifydb_type::Result;

use crate::{
	TransactionId,
	change::RowChange,
	single::{read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::{admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction},
};

pub mod admin;
pub mod catalog;
pub mod command;
pub mod query;

/// An enum that can hold either a command, admin, or query transaction for flexible
/// execution
pub enum Transaction<'a> {
	Command(&'a mut CommandTransaction),
	Admin(&'a mut AdminTransaction),
	Query(&'a mut QueryTransaction),
}

impl<'a> Transaction<'a> {
	/// Get the transaction version
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Command(txn) => txn.version(),
			Self::Admin(txn) => txn.version(),
			Self::Query(txn) => txn.version(),
		}
	}

	/// Get the transaction ID
	pub fn id(&self) -> TransactionId {
		match self {
			Self::Command(txn) => txn.id(),
			Self::Admin(txn) => txn.id(),
			Self::Query(txn) => txn.id(),
		}
	}

	/// Get a value by key (async method)
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key),
			Self::Admin(txn) => txn.get(key),
			Self::Query(txn) => txn.get(key),
		}
	}

	/// Check if a key exists (async method)
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key),
			Self::Admin(txn) => txn.contains_key(key),
			Self::Query(txn) => txn.contains_key(key),
		}
	}

	/// Get a prefix batch (async method)
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Admin(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
		}
	}

	/// Get a reverse prefix batch (async method)
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Admin(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
		}
	}

	/// Read as of version exclusive (async method)
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Admin(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Query(txn) => txn.read_as_of_version_exclusive(version),
		}
	}

	/// Create a streaming iterator for forward range queries.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range(range, batch_size),
			Transaction::Admin(txn) => txn.range(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range(range, batch_size)),
		}
	}

	/// Create a streaming iterator for reverse range queries.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range_rev(range, batch_size),
			Transaction::Admin(txn) => txn.range_rev(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range_rev(range, batch_size)),
		}
	}
}

impl<'a> From<&'a mut CommandTransaction> for Transaction<'a> {
	fn from(txn: &'a mut CommandTransaction) -> Self {
		Self::Command(txn)
	}
}

impl<'a> From<&'a mut AdminTransaction> for Transaction<'a> {
	fn from(txn: &'a mut AdminTransaction) -> Self {
		Self::Admin(txn)
	}
}

impl<'a> From<&'a mut QueryTransaction> for Transaction<'a> {
	fn from(txn: &'a mut QueryTransaction) -> Self {
		Self::Query(txn)
	}
}

impl<'a> Transaction<'a> {
	/// Re-borrow this transaction with a shorter lifetime, enabling
	/// multiple sequential uses of the same transaction binding.
	pub fn reborrow(&mut self) -> Transaction<'_> {
		match self {
			Transaction::Command(cmd) => Transaction::Command(cmd),
			Transaction::Admin(admin) => Transaction::Admin(admin),
			Transaction::Query(qry) => Transaction::Query(qry),
		}
	}

	/// Extract the underlying CommandTransaction, panics if this is
	/// not a Command transaction
	pub fn command(self) -> &'a mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			Self::Admin(_) => panic!("Expected Command transaction but found Admin transaction"),
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Extract the underlying AdminTransaction, panics if this is
	/// not an Admin transaction
	pub fn admin(self) -> &'a mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Command(_) => panic!("Expected Admin transaction but found Command transaction"),
			Self::Query(_) => panic!("Expected Admin transaction but found Query transaction"),
		}
	}

	/// Extract the underlying QueryTransaction, panics if this is a
	/// Command transaction
	pub fn query(self) -> &'a mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			Self::Command(_) => panic!("Expected Query transaction but found Command transaction"),
			Self::Admin(_) => panic!("Expected Query transaction but found Admin transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// CommandTransaction, panics if this is not a Command transaction
	pub fn command_mut(&mut self) -> &mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			Self::Admin(_) => panic!("Expected Command transaction but found Admin transaction"),
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// AdminTransaction, panics if this is not an Admin transaction
	pub fn admin_mut(&mut self) -> &mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Command(_) => panic!("Expected Admin transaction but found Command transaction"),
			Self::Query(_) => panic!("Expected Admin transaction but found Query transaction"),
		}
	}

	/// Get a mutable reference to the underlying QueryTransaction,
	/// panics if this is not a Query transaction
	pub fn query_mut(&mut self) -> &mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			Self::Command(_) => panic!("Expected Query transaction but found Command transaction"),
			Self::Admin(_) => panic!("Expected Query transaction but found Admin transaction"),
		}
	}

	/// Begin a single-version query transaction for specific keys
	pub fn begin_single_query<'b, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_query(keys),
			Transaction::Admin(txn) => txn.begin_single_query(keys),
			Transaction::Query(txn) => txn.begin_single_query(keys),
		}
	}

	/// Begin a single-version write transaction for specific keys.
	/// Panics on Query transactions.
	pub fn begin_single_command<'b, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_command(keys),
			Transaction::Admin(txn) => txn.begin_single_command(keys),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	/// Set a key-value pair. Panics on Query transactions.
	pub fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.set(key, row),
			Transaction::Admin(txn) => txn.set(key, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	/// Unset (delete with tombstone) a key-value pair. Panics on Query transactions.
	pub fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.unset(key, values),
			Transaction::Admin(txn) => txn.unset(key, values),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	/// Remove a key. Panics on Query transactions.
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove(key),
			Transaction::Admin(txn) => txn.remove(key),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	/// Track a row change for post-commit event emission. Panics on Query transactions.
	pub fn track_row_change(&mut self, change: RowChange) {
		match self {
			Transaction::Command(txn) => txn.track_row_change(change),
			Transaction::Admin(txn) => txn.track_row_change(change),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
