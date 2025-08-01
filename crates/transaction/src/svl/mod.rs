// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::delta::Delta;
use reifydb_core::interface::{Unversioned, UnversionedStorage, WriteTransaction};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlReadTransaction;
pub use write::SvlWriteTransaction;

#[derive(Clone)]
pub struct SingleVersionLock<US> {
    inner: Arc<SvlInner<US>>,
}

struct SvlInner<US> {
    storage: RwLock<US>,
    write_active: AtomicBool,
}

impl<US> SingleVersionLock<US>
where
    US: UnversionedStorage,
{
    pub fn new(storage: US) -> Self {
        Self {
            inner: Arc::new(SvlInner {
                storage: RwLock::new(storage),
                write_active: AtomicBool::new(false),
            }),
        }
    }

    pub fn begin_read(&self) -> crate::Result<SvlReadTransaction<'_, US>> {
        let storage = self.inner.storage.read().unwrap();
        Ok(SvlReadTransaction { storage })
    }

    pub fn begin_write(&self) -> crate::Result<SvlWriteTransaction<US>> {
        // Try to acquire write lock atomically
        match self.inner.write_active.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(SvlWriteTransaction::new(self.inner.clone())),
            Err(_) => {
                panic!("Write transaction already active")
            }
        }
    }

    pub fn with_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&US) -> crate::Result<R>,
    {
        let tx = self.begin_read()?;
        f(&*tx.storage)
    }

    pub fn with_write<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut SvlWriteTransaction<US>) -> crate::Result<R>,
    {
        let mut tx = self.begin_write()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
}
