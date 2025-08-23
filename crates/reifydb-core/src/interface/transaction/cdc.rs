// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use crate::{
    interface::CdcEvent, Result,
    Version,
};

pub trait CdcTransaction: Send + Sync + Clone + 'static {
	type Query<'a>: CdcQueryTransaction;

	fn begin_query(&self) -> Result<Self::Query<'_>>;

	fn with_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut Self::Query<'_>) -> Result<R>,
	{
		let mut tx = self.begin_query()?;
		f(&mut tx)
	}
}

pub trait CdcQueryTransaction: Send + Sync + Clone + 'static {
	fn get(&self, version: Version) -> Result<Vec<CdcEvent>>;

	fn range(
		&self,
		start: Bound<Version>,
		end: Bound<Version>,
	) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>>;

	fn scan(&self) -> Result<Box<dyn Iterator<Item = CdcEvent> + '_>>;

	fn count(&self, version: Version) -> Result<usize>;
}
