// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::memory::Memory;
use crate::{Contains, Version};
use reifydb_persistence::Key;
use std::collections::Bound;

impl Contains for Memory {
    fn contains(&self, key: &Key, version: Version) -> bool {
        match self.memory.get(key) {
            None => false,
            Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
                None => false,
                Some(item) => item.value().is_some(),
            },
        }
    }
}
