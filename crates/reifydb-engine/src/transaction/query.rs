// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::interface::TransactionId;
use reifydb_core::{
	interface::{
		BoxedVersionedIter, CdcTransaction, QueryTransaction,
		Transaction, UnversionedTransaction, Versioned,
		VersionedQueryTransaction, VersionedTransaction,
	}, EncodedKey,
	EncodedKeyRange,
};

/// An active query transaction that holds a versioned query transaction
/// and provides query-only access to unversioned storage.
pub struct StandardQueryTransaction<T: Transaction> {
	versioned: <T::Versioned as VersionedTransaction>::Query,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

impl<T: Transaction> StandardQueryTransaction<T> {
	/// Creates a new active query transaction
	pub fn new(
		versioned: <T::Versioned as VersionedTransaction>::Query,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
	) -> Self {
		Self {
			versioned,
			unversioned,
			cdc,
			_not_send_sync: PhantomData,
		}
	}

	/// Execute a function with query access to the unversioned transaction.
	pub fn with_unversioned_query<F, R>(&self, f: F) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Query<
				'_,
			>,
		) -> reifydb_core::Result<R>,
	{
		self.unversioned.with_query(f)
	}

	/// Execute a function with access to the versioned query transaction.
	/// This operates within the same transaction context.
	pub fn with_versioned_query<F, R>(&mut self, f: F) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Query,
		) -> reifydb_core::Result<R>,
	{
		f(&mut self.versioned)
	}

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}
}

impl<T: Transaction> VersionedQueryTransaction for StandardQueryTransaction<T> {
	#[inline]
	fn version(&self) -> reifydb_core::Version {
		self.versioned.version()
	}

	#[inline]
	fn id(&self) -> TransactionId {
		self.versioned.id()
	}

	#[inline]
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> reifydb_core::Result<Option<Versioned>> {
		self.versioned.get(key)
	}

	#[inline]
	fn contains_key(&mut self, key: &EncodedKey) -> reifydb_core::Result<bool> {
		self.versioned.contains_key(key)
	}

	#[inline]
	fn scan(&mut self) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.scan()
	}

	#[inline]
	fn scan_rev(&mut self) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.scan_rev()
	}

	#[inline]
	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.range(range)
	}

	#[inline]
	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.range_rev(range)
	}

	#[inline]
	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.prefix(prefix)
	}

	#[inline]
	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.versioned.prefix_rev(prefix)
	}
}

impl<T: Transaction> QueryTransaction for StandardQueryTransaction<T> {
	type UnversionedQuery<'a> =
		<T::Unversioned as UnversionedTransaction>::Query<'a>;
	type CdcQuery<'a> = <T::Cdc as CdcTransaction>::Query<'a>;

	fn begin_unversioned_query(
		&self,
	) -> reifydb_core::Result<Self::UnversionedQuery<'_>> {
		self.unversioned.begin_query()
	}

	fn begin_cdc_query(&self) -> reifydb_core::Result<Self::CdcQuery<'_>> {
		self.cdc.begin_query()
	}
}
