// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{UnversionedStorage, VersionedStorage};
use std::marker::PhantomData;

mod active;
mod unversioned;
mod versioned;

pub use active::*;
pub use unversioned::*;
pub use versioned::*;

pub struct Transaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub versioned: VT,
    pub unversioned: UT,
    _phantom: PhantomData<(VS, US)>,
}

impl<VS, US, VT, UT> Transaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    pub fn begin_query(&self) -> crate::Result<VT::Query> {
        self.versioned.begin_query()
    }

    #[inline]
    pub fn begin_command(&self) -> crate::Result<VT::Command> {
        self.versioned.begin_command()
    }

    #[inline]
    pub fn with_query<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut VT::Query) -> crate::Result<R>,
    {
        self.versioned.with_query(f)
    }

    #[inline]
    pub fn with_command<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut VT::Command) -> crate::Result<R>,
    {
        self.versioned.with_command(f)
    }

    #[inline]
    pub fn begin_query_unversioned(&self) -> crate::Result<UT::Query<'_>> {
        self.unversioned.begin_query()
    }

    #[inline]
    pub fn begin_command_unversioned(&self) -> crate::Result<UT::Command<'_>> {
        self.unversioned.begin_command()
    }

    #[inline]
    pub fn with_query_unversioned<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Query<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_query(f)
    }

    #[inline]
    pub fn with_command_unversioned<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Command<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_command(f)
    }
}
