// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Apply;
use crate::memory::Memory;
use crate::memory::versioned::Versioned;
use reifydb_core::Version;
use reifydb_core::delta::Delta;

impl Apply for Memory {
    fn apply(&self, delta: Vec<Delta>, version: Version) {
        for delta in delta {
            match delta {
                Delta::Set { key, bytes } => {
                    let item = self.memory.get_or_insert_with(key, || Versioned::new());
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(bytes));
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
