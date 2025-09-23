// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::change::TransactionalDefChanges;
use crate::interface::{
	CdcQueryTransaction, MultiVersionCommandTransaction, MultiVersionQueryTransaction,
	SingleVersionCommandTransaction, SingleVersionQueryTransaction,
};

pub trait CommandTransaction: MultiVersionCommandTransaction + QueryTransaction {
	type SingleVersionCommand<'a>: SingleVersionCommandTransaction
	where
		Self: 'a;

	fn begin_single_command(&self) -> crate::Result<Self::SingleVersionCommand<'_>>;

	fn with_single_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::SingleVersionCommand<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_single_command()?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}

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

	fn begin_single_query(&self) -> crate::Result<Self::SingleVersionQuery<'_>>;

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>>;

	fn with_single_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::SingleVersionQuery<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_single_query()?;
		let result = f(&mut tx)?;
		Ok(result)
	}

	fn with_cdc_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::CdcQuery<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_cdc_query()?;
		let result = f(&mut tx)?;
		Ok(result)
	}
}
