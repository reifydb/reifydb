// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;

use super::change::TransactionalDefChanges;
use crate::{
	EncodedKey,
	interface::{
		CdcQueryTransaction, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
		SingleVersionCommandTransaction, SingleVersionQueryTransaction,
	},
};

#[async_trait]
pub trait CommandTransaction: MultiVersionCommandTransaction + QueryTransaction {
	type SingleVersionCommand<'a>: SingleVersionCommandTransaction
	where
		Self: 'a;

	async fn begin_single_command<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionCommand<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send;

	/// Get reference to catalog changes for this transaction
	fn get_changes(&self) -> &TransactionalDefChanges;
}

#[async_trait]
pub trait QueryTransaction: MultiVersionQueryTransaction {
	type SingleVersionQuery<'a>: SingleVersionQueryTransaction
	where
		Self: 'a;

	type CdcQuery<'a>: CdcQueryTransaction
	where
		Self: 'a;

	async fn begin_single_query<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send;

	async fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>>;
}
