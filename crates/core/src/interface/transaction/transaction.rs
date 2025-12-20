// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::change::TransactionalDefChanges;
use crate::{
	EncodedKey,
	interface::{
		CdcQueryTransaction, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
		SingleVersionCommandTransaction, SingleVersionQueryTransaction,
	},
};

pub trait CommandTransaction: MultiVersionCommandTransaction + QueryTransaction {
	type SingleVersionCommand<'a>: SingleVersionCommandTransaction
	where
		Self: 'a;

	fn begin_single_command<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionCommand<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>;

	/// Get reference to catalog changes for this transaction
	fn get_changes(&self) -> &TransactionalDefChanges;
}

pub trait QueryTransaction: MultiVersionQueryTransaction {
	type SingleVersionQuery<'a>: SingleVersionQueryTransaction
	where
		Self: 'a;

	type CdcQuery<'a>: CdcQueryTransaction
	where
		Self: 'a;

	fn begin_single_query<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>;

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>>;
}
