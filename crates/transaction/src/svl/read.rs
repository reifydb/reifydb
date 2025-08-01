// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

use std::sync::RwLockReadGuard;

pub struct ReadTransaction<'a, US> {
    pub(super) storage: RwLockReadGuard<'a, US>,
}

impl<US> ReadTransaction<'_, US>
where
    US: UnversionedStorage,
{
    pub fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        self.storage.get(key)
    }

    pub fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        self.storage.contains(key)
    }

    // For scan operations, we need to collect into a Vec to ensure Send
    pub fn scan(&mut self) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan()?;
        Ok(Box::new(iter.into_iter()))
    }

    pub fn scan_rev(&mut self) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan_rev()?;
        Ok(Box::new(iter.into_iter()))
    }

    pub fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan_range(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    pub fn scan_prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan_prefix(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }

    pub fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan_range_rev(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    pub fn scan_prefix_rev(
        &mut self,
        prefix: &EncodedKey,
    ) -> crate::Result<BoxedUnversionedIter<'_>> {
        let iter = self.storage.scan_prefix_rev(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }
}
