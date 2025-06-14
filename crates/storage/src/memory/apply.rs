// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::VersionedApply;
use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use reifydb_core::delta::Delta;
use reifydb_core::{AsyncCowVec, Version};

impl VersionedApply for Memory {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) {
        for delta in delta {
            match delta {
                Delta::Set { key, row: row } => {
                    let item = self.versioned.get_or_insert_with(key, || VersionedRow::new());
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(row));
                    val.unlock();
                }
                Delta::Remove { key } => {
                    if let Some(values) = self.versioned.get(&key) {
                        let values = values.value();
                        if !values.is_empty() {
                            values.insert(version, None);
                        }
                    }
                }
            }
        }
    }
}
