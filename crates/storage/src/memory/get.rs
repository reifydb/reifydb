// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::memory::Memory;
use crate::{VersionedGet, Versioned};
use reifydb_core::{EncodedKey, Version};
use std::collections::Bound;

impl VersionedGet for Memory {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned> {
        let item = self.versioned.get(key)?;
        let (version, value) =
            item.value().upper_bound(Bound::Included(&version)).and_then(|v| {
                if v.value().is_some() {
                    Some((*v.key(), v.value().clone().unwrap()))
                } else {
                    None
                }
            })?;

        Some(Versioned { key: key.clone(), row: value, version })
    }
}
