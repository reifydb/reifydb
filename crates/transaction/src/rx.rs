// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::Versioned;

pub type VersionedIter<'a> = Box<dyn Iterator<Item = Versioned> + 'a>;

pub trait Rx {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>>;

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&mut self) -> crate::Result<VersionedIter>;

    fn scan_rev(&mut self) -> crate::Result<VersionedIter>;

    fn scan_range(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter>;

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter>;

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter>;

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter>;
}
