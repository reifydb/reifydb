// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::VersionedApply;
use crate::lmdb::Lmdb;
use reifydb_core::delta::Delta;
use reifydb_core::{AsyncCowVec, Version};
use crate::unversioned::UnversionedApply;

impl VersionedApply for Lmdb {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) {
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

impl UnversionedApply for Lmdb{
    fn apply(&self, delta: AsyncCowVec<Delta>) {
        todo!()
    }
}