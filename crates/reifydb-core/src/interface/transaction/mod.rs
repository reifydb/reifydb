// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod cdc;
mod command;
pub mod interceptor;
mod pending;
mod query;
mod underlying;
mod unversioned;
mod versioned;

use std::marker::PhantomData;

pub use cdc::{
	CdcQueryTransaction, CdcTransaction, StandardCdcQueryTransaction,
	StandardCdcTransaction,
};
pub use command::CommandTransaction;
pub use pending::PendingWrite;
pub use query::QueryTransaction;
pub use underlying::{
	UnderlyingCommandTransaction, UnderlyingQueryTransaction,
};
pub use unversioned::*;
pub use versioned::*;

pub trait Transaction: Send + Sync + Clone + 'static {
	type Versioned: VersionedTransaction;
	type Unversioned: UnversionedTransaction;
	type Cdc: CdcTransaction;
}

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
