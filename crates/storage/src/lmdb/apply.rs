// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Action, Apply, Version};

impl Apply for Lmdb {
    fn apply(&self, actions: Vec<(Action, Version)>) {
        let mut tx = self.env.write_txn().unwrap();
        for (action, version) in actions {
            match action {
                Action::Set { key, value } => {
                    self.db.put(&mut tx, &key[..], &value).unwrap();
                }
                Action::Remove { key } => {
                    self.db.delete(&mut tx, &key[..]).unwrap();
                }
            }
        }
        tx.commit().unwrap();
    }
}
