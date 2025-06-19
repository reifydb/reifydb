// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{Unversioned, UnversionedStorage};

pub struct BypassRx<US: UnversionedStorage> {
    unversioned: US,
}

impl<US: UnversionedStorage> BypassRx<US> {
    pub fn new(unversioned: US) -> BypassRx<US> {
        Self { unversioned }
    }
}
impl<US: UnversionedStorage> BypassRx<US> {
    pub fn get(&self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        Ok(self.unversioned.get_unversioned(key))
    }
}

// allows to bypass the transaction mechanism and write directly to the unversioned storage
pub struct BypassTx<US: UnversionedStorage> {
    unversioned: US,
}

impl<US: UnversionedStorage> BypassTx<US> {
    pub fn new(unversioned: US) -> Self {
        Self { unversioned }
    }
}

impl<US: UnversionedStorage> BypassTx<US> {
    pub fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        Ok(self.unversioned.get_unversioned(key))
    }

    pub fn scan(&mut self) -> crate::Result<US::ScanIter<'_>> {
        Ok(self.unversioned.scan_unversioned())
    }

    pub fn scan_range(&mut self, range: EncodedKeyRange) -> crate::Result<US::ScanRange<'_>> {
        Ok(self.unversioned.scan_range_unversioned(range))
    }

    pub fn scan_prefix(&mut self, key: &EncodedKey) -> crate::Result<US::ScanRange<'_>> {
        Ok(self.unversioned.scan_prefix_unversioned(key))
    }

    pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        self.unversioned.set_unversioned(key, row);
        Ok(())
    }

    pub fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        self.unversioned.remove_unversioned(key);
        Ok(())
    }
}
