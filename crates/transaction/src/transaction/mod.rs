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
	single::read::SingleReadTransaction,
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

pub trait AsTransaction: Send {
	fn as_transaction(&mut self) -> Transaction<'_>;
}

impl AsTransaction for CommandTransaction {
	fn as_transaction(&mut self) -> Transaction<'_> {
		Transaction::Command(self)
	}
}

impl AsTransaction for AdminTransaction {
	fn as_transaction(&mut self) -> Transaction<'_> {
		Transaction::Admin(self)
	}
}

impl AsTransaction for QueryTransaction {
	fn as_transaction(&mut self) -> Transaction<'_> {
		Transaction::Query(self)
	}
}

impl AsTransaction for Transaction<'_> {
	fn as_transaction(&mut self) -> Transaction<'_> {
		match self {
			Transaction::Command(cmd) => Transaction::Command(cmd),
			Transaction::Admin(admin) => Transaction::Admin(admin),
			Transaction::Query(qry) => Transaction::Query(qry),
		}
	}
}

impl<'a> Transaction<'a> {
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
}
