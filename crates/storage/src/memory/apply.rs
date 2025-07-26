// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use reifydb_core::delta::Delta;
use reifydb_core::interface::{UnversionedApply, VersionedApply};
use reifydb_core::{CowVec, Result, Version};

impl VersionedApply for Memory {
    fn apply(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
        for delta in delta {
            match delta {
                Delta::Insert { key, row }
                | Delta::Update { key, row }
                | Delta::Upsert { key, row } => {
                    let item = self.versioned.get_or_insert_with(key, VersionedRow::new);
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
        Ok(())
    }
}

impl UnversionedApply for Memory {
    fn apply(&mut self, delta: CowVec<Delta>) -> Result<()> {
        for delta in delta {
            match delta {
                Delta::Insert { key, row }
                | Delta::Update { key, row }
                | Delta::Upsert { key, row } => {
                    self.unversioned.insert(key, row);
                }
                Delta::Remove { key } => {
                    self.unversioned.remove(&key);
                }
            }
        }
        Ok(())
    }
}
