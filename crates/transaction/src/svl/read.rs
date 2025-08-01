// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

use reifydb_core::interface::{BoxedUnversionedIter, ReadTransaction};
use std::sync::RwLockReadGuard;

pub struct SvlReadTransaction<'a, US> {
    pub(super) storage: RwLockReadGuard<'a, US>,
}

impl<US> ReadTransaction for SvlReadTransaction<'_, US>
where
    US: UnversionedStorage,
{
    type Item = Unversioned;
    type Iter<'a>
        = BoxedUnversionedIter<'a>
    where
        Self: 'a;

    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Self::Item>> {
        self.storage.get(key)
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        self.storage.contains(key)
    }

    fn scan(&mut self) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan()?;
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_rev(&mut self) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan_rev()?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan_range(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan_range_rev(range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan_prefix(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<Self::Iter<'_>> {
        let iter = self.storage.scan_prefix_rev(prefix)?;
        Ok(Box::new(iter.into_iter()))
    }
}
