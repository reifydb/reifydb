// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::VersionedContains;
use crate::memory::Memory;
use crate::unversioned::UnversionedContains;
use reifydb_core::{EncodedKey, Version};
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
    fn contains_unversioned(&self, key: &EncodedKey) -> bool {
        match self.unversioned.get(key) {
            None => false,
            Some(_) => true,
        }
    }
}
