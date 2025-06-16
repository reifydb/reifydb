// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Rx;
use crate::bypass::BypassTx;
use reifydb_core::EncodedKey;
use reifydb_core::row::EncodedRow;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use std::sync::MutexGuard;

pub trait Tx<VS: VersionedStorage, US: UnversionedStorage>: Rx {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;

    fn bypass(&mut self) -> MutexGuard<BypassTx<US>>;
}
