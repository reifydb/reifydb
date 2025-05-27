// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Persistence;
use base::encoding::bincode;
use base::{Value, key_prefix};

pub trait TableExtension: Persistence {
    fn table_append_rows(
        &mut self,
        schema: &str,
        table: &str,
        rows: &[Vec<Value>],
    ) -> crate::Result<usize> {
        let last_id = self.scan_prefix(&key_prefix!("{}::{}::row::", schema, table)).count();

        let inserted = rows.len();

        for (id, row) in rows.iter().enumerate() {
            self.set(
                // &encode_key(format!("{}::row::{}", store, (last_id + id + 1)).as_str()),
                key_prefix!("{}::{}::row::{}", schema, table, (last_id + id + 1)),
                bincode::serialize(row),
            )
            .unwrap();
        }

        Ok(inserted)
    }
}
