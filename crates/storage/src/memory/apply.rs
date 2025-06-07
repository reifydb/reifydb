// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::memory::Memory;
use crate::memory::value::VersionedValues;
use crate::{Delta, Apply, Version};

impl Apply for Memory {
    fn apply(&self, actions: Vec<(Delta, Version)>) {
        for (delta, version) in actions {
            match delta {
                Delta::Set { key, value } => {
                    let item = self.memory.get_or_insert_with(key, || VersionedValues::new());
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(value));
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
