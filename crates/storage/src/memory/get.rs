// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::memory::Memory;
use crate::{Get, Stored};
use reifydb_core::{Key, Version};
use std::collections::Bound;

impl Get for Memory {
    fn get(&self, key: &Key, version: Version) -> Option<Stored> {
        let item = self.memory.get(key)?;
        let (version, value) =
            item.value().upper_bound(Bound::Included(&version)).and_then(|v| {
                if v.value().is_some() {
                    Some((*v.key(), v.value().clone().unwrap()))
                } else {
                    None
                }
            })?;

        Some(Stored { key: key.clone(), bytes: value, version })
    }
}
