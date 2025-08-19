// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{
		BoxedVersionedIter, Transaction, UnversionedTransaction,
		Versioned, VersionedQueryTransaction, VersionedTransaction,
	},
};

/// An active query transaction that holds a versioned query transaction
/// and provides query-only access to unversioned storage.
pub struct QueryTransaction<T: Transaction> {
	versioned: <T::Versioned as VersionedTransaction>::Query,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

impl<T: Transaction> QueryTransaction<T> {
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
	pub fn with_unversioned_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Query<
				'_,
			>,
		) -> crate::Result<R>,
	{
		self.unversioned.with_query(f)
	}

	/// Execute a function with access to the versioned query transaction.
	/// This operates within the same transaction context.
	pub fn with_versioned_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Query,
		) -> crate::Result<R>,
	{
		f(&mut self.versioned)
	}

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}
}

impl<T: Transaction> VersionedQueryTransaction for QueryTransaction<T> {
	#[inline]
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> crate::Result<Option<Versioned>> {
		self.versioned.get(key)
	}

	#[inline]
	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.versioned.contains_key(key)
	}

	#[inline]
	fn scan(&mut self) -> crate::Result<BoxedVersionedIter> {
		self.versioned.scan()
	}

	#[inline]
	fn scan_rev(&mut self) -> crate::Result<BoxedVersionedIter> {
		self.versioned.scan_rev()
	}

	#[inline]
	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter> {
		self.versioned.range(range)
	}

	#[inline]
	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter> {
		self.versioned.range_rev(range)
	}

	#[inline]
	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter> {
		self.versioned.prefix(prefix)
	}

	#[inline]
	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter> {
		self.versioned.prefix_rev(prefix)
	}
}
