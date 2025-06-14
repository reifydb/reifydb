// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::EncodedKey;
use reifydb_core::row::EncodedRow;
use reifydb_storage::{Unversioned, UnversionedStorage};

pub struct BypassRx<US: UnversionedStorage> {
    storage: US,
}

impl<US: UnversionedStorage> BypassRx<US> {
    pub fn get(&self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        Ok(self.storage.get_unversioned(key))
    }
}

// allows to bypass the transaction mechanism and write directly to the unversioned storage
pub struct BypassTx<US: UnversionedStorage> {
    storage: US,
}

impl<US: UnversionedStorage> BypassTx<US> {
    pub fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        Ok(self.storage.get_unversioned(key))
    }

    pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        Ok(self.storage.set_unversioned(key, row))
    }

    pub fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        Ok(self.storage.remove_unversioned(key))
    }
}
