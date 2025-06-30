// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::interface::{Bypass, Unversioned, UnversionedStorage};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Error};

// allows to bypass the transaction mechanism and write directly to the unversioned storage
pub struct BypassTx<US: UnversionedStorage> {
    unversioned: US,
}

impl<US: UnversionedStorage> BypassTx<US> {
    pub fn new(unversioned: US) -> Self {
        Self { unversioned }
    }
}

impl<US: UnversionedStorage> Bypass<US> for BypassTx<US> {
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Unversioned>, Error> {
        Ok(self.unversioned.get_unversioned(key))
    }

    fn scan(&mut self) -> Result<US::ScanIter<'_>, Error> {
        Ok(self.unversioned.scan_unversioned())
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<US::ScanRange<'_>, Error> {
        Ok(self.unversioned.scan_range_unversioned(range))
    }

    fn scan_prefix(&mut self, key: &EncodedKey) -> Result<US::ScanRange<'_>, Error> {
        Ok(self.unversioned.scan_prefix_unversioned(key))
    }

    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error> {
        self.unversioned.set_unversioned(key, row);
        Ok(())
    }

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
        self.unversioned.remove_unversioned(key);
        Ok(())
    }
}
