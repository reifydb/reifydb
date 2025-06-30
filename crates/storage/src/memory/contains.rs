// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::memory::Memory;
use reifydb_core::interface::{UnversionedContains, VersionedContains};
use reifydb_core::{EncodedKey, Error, Version};
use std::collections::Bound;

impl VersionedContains for Memory {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool {
        match self.versioned.get(key) {
            None => false,
            Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
                None => false,
                Some(item) => item.value().is_some(),
            },
        }
    }
}

impl UnversionedContains for Memory {
    fn contains(&self, key: &EncodedKey) -> Result<bool, Error> {
        Ok(self.unversioned.get(key).is_some())
    }
}
