// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use reifydb_core::delta::Delta;
use reifydb_core::interface::{UnversionedApply, VersionedApply};
use reifydb_core::{AsyncCowVec, Version};

impl VersionedApply for Lmdb {
    fn apply(&self, delta: AsyncCowVec<Delta>, _version: Version) {
        let mut tx = self.env.write_txn().unwrap();
        for delta in delta {
            match delta {
                Delta::Set { key, row: value } => {
                    self.db.put(&mut tx, &key[..], &value).unwrap();
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
    fn apply_unversioned(&mut self, delta: AsyncCowVec<Delta>) {
        let mut tx = self.env.write_txn().unwrap();
        for delta in delta {
            match delta {
                Delta::Set { key, row: value } => {
                    self.db.put(&mut tx, &key[..], &value).unwrap();
                }
                Delta::Remove { key } => {
                    self.db.delete(&mut tx, &key[..]).unwrap();
                }
            }
        }
        tx.commit().unwrap();
    }
}
