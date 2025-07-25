// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use reifydb_core::delta::Delta;
use reifydb_core::interface::{UnversionedApply, VersionedApply};
use reifydb_core::{CowVec, Error, Version};

impl VersionedApply for Lmdb {
    fn apply(&self, delta: CowVec<Delta>, _version: Version) {
        let mut tx = self.env.write_txn().unwrap();
        for delta in delta {
            match delta {
                Delta::Insert { key, row }
                | Delta::Update { key, row }
                | Delta::Upsert { key, row } => {
                    self.db.put(&mut tx, &key[..], &row).unwrap();
                }
                Delta::Remove { key } => {
                    self.db.delete(&mut tx, &key[..]).unwrap();
                }
            }
        }
        tx.commit().unwrap();
    }
}

impl UnversionedApply for Lmdb {
    fn apply(&mut self, delta: CowVec<Delta>) -> Result<(), Error> {
        let mut tx = self.env.write_txn().unwrap();
        for delta in delta {
            match delta {
                Delta::Insert { key, row }
                | Delta::Update { key, row }
                | Delta::Upsert { key, row } => {
                    self.db.put(&mut tx, &key[..], &row).unwrap();
                }
                Delta::Remove { key } => {
                    self.db.delete(&mut tx, &key[..]).unwrap();
                }
            }
        }
        tx.commit().unwrap();
        Ok(())
    }
}
