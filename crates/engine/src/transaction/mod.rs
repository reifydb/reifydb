// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, TransactionId,
	interface::{
		BoxedMultiVersionIter, MultiVersionQueryTransaction, MultiVersionValues, QueryTransaction,
		SingleVersionTransaction,
	},
};
use reifydb_transaction::single::TransactionSingleVersion;

mod catalog;
mod command;
#[allow(dead_code)]
pub(crate) mod operation;
mod query;

pub use command::StandardCommandTransaction;
pub use query::StandardQueryTransaction;
use reifydb_core::interface::CdcTransaction;
use reifydb_transaction::cdc::TransactionCdc;

/// An enum that can hold either a command or query transaction for flexible
/// execution
pub enum StandardTransaction<'a> {
	Command(&'a mut StandardCommandTransaction),
	Query(&'a mut StandardQueryTransaction),
}

impl<'a> QueryTransaction for StandardTransaction<'a> {
	type SingleVersionQuery<'b>
		= <TransactionSingleVersion as SingleVersionTransaction>::Query<'b>
	where
		Self: 'b;

	type CdcQuery<'b>
		= <TransactionCdc as CdcTransaction>::Query<'b>
	where
		Self: 'b;

	fn begin_single_query<'k, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'k EncodedKey>,
	{
		match self {
			Self::Command(txn) => txn.begin_single_query(keys),
			Self::Query(txn) => txn.begin_single_query(keys),
		}
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		match self {
			Self::Command(txn) => txn.begin_cdc_query(),
			Self::Query(txn) => txn.begin_cdc_query(),
		}
	}
}

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

	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		match self {
			Self::Command(txn) => txn.get(key),
			Self::Query(txn) => txn.get(key),
		}
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key),
			Self::Query(txn) => txn.contains_key(key),
		}
	}

	fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter<'_>> {
		match self {
			Self::Command(txn) => txn.range_batched(range, batch_size),
			Self::Query(txn) => txn.range_batched(range, batch_size),
		}
	}

	fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter<'_>> {
		match self {
			Self::Command(txn) => txn.range_rev_batched(range, batch_size),
			Self::Query(txn) => txn.range_rev_batched(range, batch_size),
		}
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter<'_>> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
		}
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter<'_>> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
		}
	}

	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> reifydb_core::Result<()> {
		match self {
			StandardTransaction::Command(txn) => txn.read_as_of_version_inclusive(version),
			StandardTransaction::Query(txn) => txn.read_as_of_version_exclusive(version),
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

	pub fn version(&self) -> CommitVersion {
		match self {
			StandardTransaction::Command(txn) => MultiVersionQueryTransaction::version(*txn),
			StandardTransaction::Query(txn) => MultiVersionQueryTransaction::version(*txn),
		}
	}
}
