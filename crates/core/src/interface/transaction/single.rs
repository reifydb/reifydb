// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{SingleVersionValues, WithEventBus},
	value::encoded::EncodedValues,
};

pub type BoxedSingleVersionIter<'a> = Box<dyn Iterator<Item = SingleVersionValues> + Send + 'a>;

pub trait SingleVersionTransaction: WithEventBus + Send + Sync + Clone + 'static {
	type Query<'a>: SingleVersionQueryTransaction;
	type Command<'a>: SingleVersionCommandTransaction;

	fn begin_query<'a, I>(&self, keys: I) -> crate::Result<Self::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>;

	fn begin_command<'a, I>(&self, keys: I) -> crate::Result<Self::Command<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>;

	fn with_query<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut Self::Query<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_query(keys)?;
		f(&mut tx)
	}

	fn with_command<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut Self::Command<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_command(keys)?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}
}

pub trait SingleVersionQueryTransaction {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait SingleVersionCommandTransaction: SingleVersionQueryTransaction {
	fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	fn commit(self) -> crate::Result<()>;

	fn rollback(self) -> crate::Result<()>;
}
