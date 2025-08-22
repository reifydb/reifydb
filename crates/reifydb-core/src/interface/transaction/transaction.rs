// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{
	CdcQueryTransaction, UnversionedCommandTransaction,
	UnversionedQueryTransaction, VersionedCommandTransaction,
	VersionedQueryTransaction,
};

pub trait CommandTransaction:
	VersionedCommandTransaction + QueryTransaction
{
	type UnversionedCommand<'a>: UnversionedCommandTransaction
	where
		Self: 'a;

	fn begin_unversioned_command(
		&self,
	) -> crate::Result<Self::UnversionedCommand<'_>>;

	fn with_unversioned_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut Self::UnversionedCommand<'_>,
		) -> crate::Result<R>,
	{
		let mut tx = self.begin_unversioned_command()?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}
}

pub trait QueryTransaction: VersionedQueryTransaction {
	type UnversionedQuery<'a>: UnversionedQueryTransaction
	where
		Self: 'a;

	type CdcQuery<'a>: CdcQueryTransaction
	where
		Self: 'a;

	fn begin_unversioned_query(
		&self,
	) -> crate::Result<Self::UnversionedQuery<'_>>;

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>>;

	fn with_unversioned_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut Self::UnversionedQuery<'_>) -> crate::Result<R>,
	{
		let mut tx = self.begin_unversioned_query()?;
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
