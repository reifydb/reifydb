// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;

use crate::{
	EncodedKey,
	interface::{SingleVersionValues, WithEventBus},
	value::encoded::EncodedValues,
};

#[async_trait]
pub trait SingleVersionTransaction: WithEventBus + Send + Sync + Clone + 'static {
	type Query<'a>: SingleVersionQueryTransaction + Send;
	type Command<'a>: SingleVersionCommandTransaction + Send;

	async fn begin_query<'a, I>(&self, keys: I) -> crate::Result<Self::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send;

	async fn begin_command<'a, I>(&self, keys: I) -> crate::Result<Self::Command<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send;
}

/// Single-version query transaction trait.
/// Uses tokio::sync locks which are Send-safe with owned guards.
#[async_trait]
pub trait SingleVersionQueryTransaction: Send {
	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;

	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;
}

/// Single-version command transaction trait.
/// Uses tokio::sync locks which are Send-safe with owned guards.
#[async_trait]
pub trait SingleVersionCommandTransaction: SingleVersionQueryTransaction + Send {
	fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()>;

	async fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	async fn commit(&mut self) -> crate::Result<()>;

	async fn rollback(&mut self) -> crate::Result<()>;
}
