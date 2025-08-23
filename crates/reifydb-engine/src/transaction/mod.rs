// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::interface::{
	CdcTransaction, Transaction, UnversionedTransaction,
	VersionedTransaction,
};

mod cdc;
mod command;
pub(crate) mod operation;
mod query;

pub use cdc::{StandardCdcQueryTransaction, StandardCdcTransaction};
pub use command::StandardCommandTransaction;
pub use query::StandardQueryTransaction;

/// A concrete implementation combining versioned and unversioned transactions
#[derive(Clone)]
pub struct StandardTransaction<V, U, C> {
	_phantom: PhantomData<(V, U, C)>,
}

impl<V, U, C> Transaction for StandardTransaction<V, U, C>
where
	V: VersionedTransaction,
	U: UnversionedTransaction,
	C: CdcTransaction,
{
	type Versioned = V;
	type Unversioned = U;
	type Cdc = C;
}
