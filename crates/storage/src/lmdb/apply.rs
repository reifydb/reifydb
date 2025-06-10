// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Apply, Delta, Version};

impl Apply for Lmdb {
    fn apply(&self, delta: Vec<Delta>, version: Version) {
        let mut tx = self.env.write_txn().unwrap();
        for delta in delta {
            match delta {
                Delta::Set { key, value } => {
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
