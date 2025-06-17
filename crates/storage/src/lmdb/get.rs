// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::unversioned::UnversionedGet;
use crate::{Unversioned, Versioned, VersionedGet};
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey, Version};

impl VersionedGet for Lmdb {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned> {
        let txn = self.env.read_txn().unwrap(); // FIXME
        self.db.get(&txn, key).unwrap().map(|bytes| Versioned {
            key: key.clone(),
            row: EncodedRow(AsyncCowVec::new(bytes.to_vec())),
            version,
        })
    }
}

impl UnversionedGet for Lmdb {
    fn get_unversioned(&self, key: &EncodedKey) -> Option<Unversioned> {
        let txn = self.env.read_txn().unwrap(); // FIXME
        self.db.get(&txn, key).unwrap().map(|bytes| Unversioned {
            key: key.clone(),
            row: EncodedRow(AsyncCowVec::new(bytes.to_vec())),
        })
    }
}
