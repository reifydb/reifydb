// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{UnversionedContains, VersionedContains};
use reifydb_core::{EncodedKey, Result, Version};
use std::collections::Bound;

impl VersionedContains for Memory {
    fn contains(&self, key: &EncodedKey, version: Version) -> Result<bool> {
        let result = match self.versioned.get(key) {
            None => false,
            Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
                None => false,
                Some(item) => item.value().is_some(),
            },
        };
        Ok(result)
    }
}

impl UnversionedContains for Memory {
    fn contains(&self, key: &EncodedKey) -> Result<bool> {
        Ok(self.unversioned.get(key).is_some())
    }
}
