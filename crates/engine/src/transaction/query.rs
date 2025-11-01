// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_catalog::{MaterializedCatalog, transaction::MaterializedCatalogTransaction};
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{
		BoxedMultiVersionIter, CdcTransaction, MultiVersionQueryTransaction, MultiVersionTransaction,
		MultiVersionValues, QueryTransaction, SingleVersionTransaction, TransactionId, TransactionalChanges,
	},
};
use reifydb_transaction::{multi::TransactionMultiVersion, single::TransactionSingleVersion};

use crate::transaction::TransactionCdc;

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct StandardQueryTransaction {
	pub(crate) multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
	pub(crate) single: TransactionSingleVersion,
	pub(crate) cdc: TransactionCdc,
	pub(crate) catalog: MaterializedCatalog,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

impl StandardQueryTransaction {
	/// Creates a new active query transaction
	pub fn new(
		multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		catalog: MaterializedCatalog,
	) -> Self {
		Self {
			multi,
			single,
			cdc,
			catalog,
			_not_send_sync: PhantomData,
		}
	}

	/// Execute a function with query access to the single transaction.
	pub fn with_single_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <TransactionSingleVersion as SingleVersionTransaction>::Query<'_>) -> crate::Result<R>,
	{
		self.single.with_query(f)
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	pub fn with_multi_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <TransactionMultiVersion as MultiVersionTransaction>::Query) -> crate::Result<R>,
	{
		f(&mut self.multi)
	}

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}
}

impl MultiVersionQueryTransaction for StandardQueryTransaction {
	#[inline]
	fn version(&self) -> CommitVersion {
		self.multi.version()
	}

	#[inline]
	fn id(&self) -> TransactionId {
		self.multi.id()
	}

	#[inline]
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		self.multi.get(key)
	}

	#[inline]
	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.multi.contains_key(key)
	}

	#[inline]
	fn scan(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.scan()
	}

	#[inline]
	fn scan_rev(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.scan_rev()
	}

	#[inline]
	fn range_batched(&mut self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.range_batched(range, batch_size)
	}

	#[inline]
	fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.range_rev_batched(range, batch_size)
	}

	#[inline]
	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.prefix(prefix)
	}

	#[inline]
	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.prefix_rev(prefix)
	}

	#[inline]
	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.multi.read_as_of_version_exclusive(version)
	}
}

impl QueryTransaction for StandardQueryTransaction {
	type SingleVersionQuery<'a> = <TransactionSingleVersion as SingleVersionTransaction>::Query<'a>;
	type CdcQuery<'a>
		= <TransactionCdc as CdcTransaction>::Query<'a>
	where
		Self: 'a;

	fn begin_single_query(&self) -> crate::Result<Self::SingleVersionQuery<'_>> {
		self.single.begin_query()
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.cdc.begin_query()
	}
}

impl MaterializedCatalogTransaction for StandardQueryTransaction {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

impl TransactionalChanges for StandardQueryTransaction {}
