// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod active;
mod unversioned;
mod versioned;

pub use active::*;
use std::marker::PhantomData;
pub use unversioned::*;
pub use versioned::*;

pub trait Transaction: Send + Sync + Clone + 'static {
    type Versioned: VersionedTransaction;
    type Unversioned: UnversionedTransaction;
}

/// A concrete implementation combining versioned and unversioned transactions
#[derive(Clone)]
pub struct StandardTransaction<V, U> {
    _phantom: PhantomData<(V, U)>,
}

impl<V, U> Transaction for StandardTransaction<V, U>
where
    V: VersionedTransaction,
    U: UnversionedTransaction,
{
    type Versioned = V;
    type Unversioned = U;
}
