// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::delta::Delta;
use reifydb_core::interface::{NewTransaction, Unversioned, UnversionedStorage};
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
}

impl<US> NewTransaction for SingleVersionLock<US>
where
    US: UnversionedStorage,
{
    type Read = SvlWriteTransaction<US>; // We'll use the write transaction for reads too
    type Write = SvlWriteTransaction<US>;

    fn begin_read(&self) -> crate::Result<Self::Read> {
        // For simplicity, we'll just use a write transaction for reads
        // This is not ideal but solves the lifetime issue
        self.begin_write()
    }

    fn begin_write(&self) -> crate::Result<Self::Write> {
        self.begin_write()
    }
}
