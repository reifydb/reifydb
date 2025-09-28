// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_catalog::{MaterializedCatalog, transaction::MaterializedCatalogTransaction};
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{
		BoxedMultiVersionIter, CdcTransaction, MultiVersionQueryTransaction, MultiVersionRow,
		MultiVersionTransaction, QueryTransaction, SingleVersionTransaction, Transaction, TransactionId,
		TransactionalChanges,
	},
};

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct StandardQueryTransaction<T: Transaction> {
	pub(crate) multi: <T::MultiVersion as MultiVersionTransaction>::Query,
	pub(crate) single: T::SingleVersion,
	pub(crate) cdc: T::Cdc,
	pub(crate) catalog: MaterializedCatalog,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

impl<T: Transaction> StandardQueryTransaction<T> {
	/// Creates a new active query transaction
	pub fn new(
		multi: <T::MultiVersion as MultiVersionTransaction>::Query,
		single: T::SingleVersion,
		cdc: T::Cdc,
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
		F: FnOnce(&mut <T::SingleVersion as SingleVersionTransaction>::Query<'_>) -> crate::Result<R>,
	{
		self.single.with_query(f)
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	pub fn with_multi_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <T::MultiVersion as MultiVersionTransaction>::Query) -> crate::Result<R>,
	{
		f(&mut self.multi)
	}

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}
}

impl<T: Transaction> MultiVersionQueryTransaction for StandardQueryTransaction<T> {
	#[inline]
	fn version(&self) -> CommitVersion {
		self.multi.version()
	}

	#[inline]
	fn id(&self) -> TransactionId {
		self.multi.id()
	}

	#[inline]
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionRow>> {
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
	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.range(range)
	}

	#[inline]
	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.range_rev(range)
	}

	#[inline]
	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.prefix(prefix)
	}

	#[inline]
	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.multi.prefix_rev(prefix)
	}
}

impl<T: Transaction> QueryTransaction for StandardQueryTransaction<T> {
	type SingleVersionQuery<'a> = <T::SingleVersion as SingleVersionTransaction>::Query<'a>;
	type CdcQuery<'a>
		= <T::Cdc as CdcTransaction>::Query<'a>
	where
		Self: 'a;

	fn begin_single_query(&self) -> crate::Result<Self::SingleVersionQuery<'_>> {
		self.single.begin_query()
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.cdc.begin_query()
	}
}

impl<T: Transaction> MaterializedCatalogTransaction for StandardQueryTransaction<T> {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

impl<T: Transaction> TransactionalChanges for StandardQueryTransaction<T> {}
