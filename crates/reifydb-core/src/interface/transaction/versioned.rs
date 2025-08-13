// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{GetHooks, Versioned},
	row::EncodedRow,
};

pub type BoxedVersionedIter<'a> =
	Box<dyn Iterator<Item = Versioned> + Send + 'a>;

pub trait VersionedTransaction:
	GetHooks + Send + Sync + Clone + 'static
{
	type Query: VersionedQueryTransaction;
	type Command: VersionedCommandTransaction;

	fn begin_query(&self) -> crate::Result<Self::Query>;

	fn begin_command(&self) -> crate::Result<Self::Command>;

	fn with_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Query) -> crate::Result<R>,
	{
		let mut tx = self.begin_query()?;
		f(&mut tx)
	}

	fn with_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Command) -> crate::Result<R>,
	{
		let mut tx = self.begin_command()?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}
}

pub trait VersionedQueryTransaction {
	fn get(&mut self, key: &EncodedKey)
	-> crate::Result<Option<Versioned>>;

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

	fn scan(&mut self) -> crate::Result<BoxedVersionedIter>;

	fn scan_rev(&mut self) -> crate::Result<BoxedVersionedIter>;

	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter>;

	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter>;

	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter>;

	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter>;
}

pub trait VersionedCommandTransaction: VersionedQueryTransaction {
	fn set(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	fn commit(self) -> crate::Result<()>;

	fn rollback(self) -> crate::Result<()>;
}
