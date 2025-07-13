// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use reifydb_core::interface::{Unversioned, UnversionedGet, Versioned, VersionedGet};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Error, Version};

impl VersionedGet for Lmdb {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned> {
        let txn = self.env.read_txn().unwrap(); // FIXME
        self.db.get(&txn, key).unwrap().map(|bytes| Versioned {
            key: key.clone(),
            row: EncodedRow(CowVec::new(bytes.to_vec())),
            version,
        })
    }
}

impl UnversionedGet for Lmdb {
    fn get(&self, key: &EncodedKey) -> Result<Option<Unversioned>, Error> {
        let txn = self.env.read_txn().unwrap(); // FIXME
        Ok(self.db.get(&txn, key).unwrap().map(|bytes| Unversioned {
            key: key.clone(),
            row: EncodedRow(CowVec::new(bytes.to_vec())),
        }))
    }
}
