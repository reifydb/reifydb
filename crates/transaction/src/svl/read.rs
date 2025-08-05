// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

use reifydb_core::interface::{BoxedUnversionedIter, UnversionedQueryTransaction};
use std::sync::RwLockReadGuard;

pub struct SvlReadTransaction<'a, US> {
    pub(super) storage: RwLockReadGuard<'a, US>,
}

impl<US> UnversionedQueryTransaction for SvlReadTransaction<'_, US>
where
    US: UnversionedStorage,
{
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Unversioned>> {
        self.storage.get(key)
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        self.storage.contains(key)
    }

    fn scan(&mut self) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan()?;
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_rev(&mut self) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan_rev()?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan_range(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan_range_rev(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan_prefix(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedUnversionedIter> {
        let iter = self.storage.scan_prefix_rev(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }
}
