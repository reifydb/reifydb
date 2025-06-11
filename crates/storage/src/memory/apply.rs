// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Apply;
use crate::memory::Memory;
use crate::memory::versioned::Versioned;
use reifydb_core::delta::Delta;
use reifydb_core::{AsyncCowVec, Version};

impl Apply for Memory {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) {
        for delta in delta {
            match delta {
                Delta::Set { key, row: row } => {
                    let item = self.memory.get_or_insert_with(key, || Versioned::new());
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(row));
                    val.unlock();
                }
                Delta::Remove { key } => {
                    if let Some(values) = self.memory.get(&key) {
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
