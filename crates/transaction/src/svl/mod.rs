// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::delta::Delta;
use reifydb_core::interface::{GetHooks, Unversioned, UnversionedStorage, UnversionedTransaction};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange};
use std::sync::{Arc, RwLock};

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlReadTransaction;
use reifydb_core::hook::Hooks;
pub use write::SvlWriteTransaction;

#[derive(Clone)]
pub struct SingleVersionLock<US> {
    inner: Arc<SvlInner<US>>,
}

struct SvlInner<US> {
    storage: RwLock<US>,
    hooks: Hooks,
}

impl<US> SingleVersionLock<US>
where
    US: UnversionedStorage,
{
    pub fn new(storage: US, hooks: Hooks) -> Self {
        Self { inner: Arc::new(SvlInner { storage: RwLock::new(storage), hooks }) }
    }
}

impl<US> GetHooks for SingleVersionLock<US>
where
    US: UnversionedStorage,
{
    fn get_hooks(&self) -> &Hooks {
        &self.inner.hooks
    }
}

impl<US> UnversionedTransaction for SingleVersionLock<US>
where
    US: UnversionedStorage,
{
    type Read<'a> = SvlReadTransaction<'a, US>;
    type Write<'a> = SvlWriteTransaction<'a, US>;

    fn begin_read(&self) -> crate::Result<Self::Read<'_>> {
        let storage = self.inner.storage.read().unwrap();
        Ok(SvlReadTransaction { storage })
    }

    fn begin_write(&self) -> crate::Result<Self::Write<'_>> {
        let storage = self.inner.storage.write().unwrap();
        Ok(SvlWriteTransaction::new(storage))
    }
}
