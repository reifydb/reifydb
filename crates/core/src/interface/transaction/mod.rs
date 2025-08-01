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
    pub fn begin_read(&self) -> crate::Result<VT::Read> {
        self.versioned.begin_read()
    }

    #[inline]
    pub fn begin_write(&self) -> crate::Result<VT::Write> {
        self.versioned.begin_write()
    }

    #[inline]
    pub fn with_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut VT::Read) -> crate::Result<R>,
    {
        self.versioned.with_read(f)
    }

    #[inline]
    pub fn with_write<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut VT::Write) -> crate::Result<R>,
    {
        self.versioned.with_write(f)
    }

    #[inline]
    pub fn begin_read_unversioned(&self) -> crate::Result<UT::Read<'_>> {
        self.unversioned.begin_read()
    }

    #[inline]
    pub fn begin_write_unversioned(&self) -> crate::Result<UT::Write<'_>> {
        self.unversioned.begin_write()
    }

    #[inline]
    pub fn with_read_unversioned<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Read<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_read(f)
    }

    #[inline]
    pub fn with_write_unversioned<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Write<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_write(f)
    }
}
