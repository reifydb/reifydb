// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod svl;

use async_trait::async_trait;
use reifydb_core::{
	EncodedKey,
	event::EventBus,
	interface::{SingleVersionCommandTransaction, SingleVersionTransaction, WithEventBus},
};
use reifydb_store_transaction::TransactionStore;
pub use svl::{SvlCommandTransaction, SvlQueryTransaction, TransactionSvl};

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionSingleVersion {
	SingleVersionLock(TransactionSvl) = 0,
}

impl TransactionSingleVersion {
	pub fn svl(store: TransactionStore, bus: EventBus) -> Self {
		Self::SingleVersionLock(TransactionSvl::new(store, bus))
	}
}

impl TransactionSingleVersion {
	pub fn testing() -> Self {
		Self::SingleVersionLock(TransactionSvl::new(TransactionStore::testing_memory(), EventBus::default()))
	}

	/// Async helper for single-version queries.
	pub async fn with_query<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <Self as SingleVersionTransaction>::Query<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_query(keys).await?;
		f(&mut tx)
	}

	/// Async helper for single-version commands.
	pub async fn with_command<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <Self as SingleVersionTransaction>::Command<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_command(keys).await?;
		let result = f(&mut tx)?;
		tx.commit().await?;
		Ok(result)
	}
}

impl WithEventBus for TransactionSingleVersion {
	fn event_bus(&self) -> &EventBus {
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.event_bus(),
		}
	}
}

#[async_trait]
impl SingleVersionTransaction for TransactionSingleVersion {
	type Query<'a> = SvlQueryTransaction<'a>;
	type Command<'a> = SvlCommandTransaction<'a>;

	#[inline]
	async fn begin_query<'a, I>(&self, keys: I) -> reifydb_core::Result<Self::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.begin_query(keys).await,
		}
	}

	#[inline]
	async fn begin_command<'a, I>(&self, keys: I) -> reifydb_core::Result<Self::Command<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.begin_command(keys).await,
		}
	}
}
