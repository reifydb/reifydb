// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey, EncodedKeyRange,
	interface::{SingleVersionValues, WithEventBus},
	value::encoded::EncodedValues,
};

pub type BoxedSingleVersionIter<'a> = Box<dyn Iterator<Item = SingleVersionValues> + Send + 'a>;

pub trait SingleVersionTransaction: WithEventBus + Send + Sync + Clone + 'static {
	type Query<'a>: SingleVersionQueryTransaction;
	type Command<'a>: SingleVersionCommandTransaction;

	fn begin_query(&self) -> crate::Result<Self::Query<'_>>;

	fn begin_command(&self) -> crate::Result<Self::Command<'_>>;

	fn with_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Query<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_query()?;
		f(&mut tx)
	}

	fn with_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::Command<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_command()?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}
}

pub trait SingleVersionQueryTransaction {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter>;

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter>;

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		self.range(EncodedKeyRange::prefix(prefix))
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionCommandTransaction: SingleVersionQueryTransaction {
	fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	fn commit(self) -> crate::Result<()>;

	fn rollback(self) -> crate::Result<()>;
}
