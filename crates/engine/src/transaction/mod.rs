// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{
		BoxedMultiVersionIter, CdcTransaction, MultiVersionQueryTransaction, MultiVersionRow,
		MultiVersionTransaction, QueryTransaction, SingleVersionTransaction, Transaction, TransactionId,
	},
};

mod catalog;
mod cdc;
mod command;
#[allow(dead_code)]
pub(crate) mod operation;
mod query;

pub use cdc::{StandardCdcQueryTransaction, StandardCdcTransaction};
pub use command::StandardCommandTransaction;
pub use query::StandardQueryTransaction;
use reifydb_catalog::MaterializedCatalog;

#[derive(Clone)]
pub struct EngineTransaction<V, U, C> {
	_phantom: PhantomData<(V, U, C)>,
}

impl<V, U, C> Transaction for EngineTransaction<V, U, C>
where
	V: MultiVersionTransaction,
	U: SingleVersionTransaction,
	C: CdcTransaction,
{
	type MultiVersion = V;
	type SingleVersion = U;
	type Cdc = C;
}

/// An enum that can hold either a command or query transaction for flexible
/// execution
pub enum StandardTransaction<'a, T: Transaction> {
	Command(&'a mut StandardCommandTransaction<T>),
	Query(&'a mut StandardQueryTransaction<T>),
}

impl<'a, T: Transaction> QueryTransaction for StandardTransaction<'a, T> {
	type SingleVersionQuery<'b>
		= <T::SingleVersion as SingleVersionTransaction>::Query<'b>
	where
		Self: 'b;

	type CdcQuery<'b>
		= <T::Cdc as CdcTransaction>::Query<'b>
	where
		Self: 'b;

	fn begin_single_query(&self) -> crate::Result<Self::SingleVersionQuery<'_>> {
		match self {
			Self::Command(txn) => txn.begin_single_query(),
			Self::Query(txn) => txn.begin_single_query(),
		}
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		match self {
			Self::Command(txn) => txn.begin_cdc_query(),
			Self::Query(txn) => txn.begin_cdc_query(),
		}
	}
}

impl<'a, T: Transaction> MultiVersionQueryTransaction for StandardTransaction<'a, T> {
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

	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionRow>> {
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

	fn scan(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.scan(),
			Self::Query(txn) => txn.scan(),
		}
	}

	fn scan_rev(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.scan_rev(),
			Self::Query(txn) => txn.scan_rev(),
		}
	}

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.range(range),
			Self::Query(txn) => txn.range(range),
		}
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.range_rev(range),
			Self::Query(txn) => txn.range_rev(range),
		}
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
		}
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
		}
	}
}

impl<'a, T: Transaction> From<&'a mut StandardCommandTransaction<T>> for StandardTransaction<'a, T> {
	fn from(txn: &'a mut StandardCommandTransaction<T>) -> Self {
		Self::Command(txn)
	}
}

impl<'a, T: Transaction> From<&'a mut StandardQueryTransaction<T>> for StandardTransaction<'a, T> {
	fn from(txn: &'a mut StandardQueryTransaction<T>) -> Self {
		Self::Query(txn)
	}
}

impl<'a, T: Transaction> StandardTransaction<'a, T> {
	/// Extract the underlying StandardCommandTransaction, panics if this is
	/// a Query transaction
	pub fn command(self) -> &'a mut StandardCommandTransaction<T> {
		match self {
			Self::Command(txn) => txn,
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Extract the underlying StandardQueryTransaction, panics if this is a
	/// Command transaction
	pub fn query(self) -> &'a mut StandardQueryTransaction<T> {
		match self {
			Self::Query(txn) => txn,
			Self::Command(_) => panic!("Expected Query transaction but found Command transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// StandardCommandTransaction, panics if this is a Query transaction
	pub fn command_mut(&mut self) -> &mut StandardCommandTransaction<T> {
		match self {
			Self::Command(txn) => txn,
			Self::Query(_) => panic!("Expected Command transaction but found Query transaction"),
		}
	}

	/// Get a mutable reference to the underlying StandardQueryTransaction,
	/// panics if this is a Command transaction
	pub fn query_mut(&mut self) -> &mut StandardQueryTransaction<T> {
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
