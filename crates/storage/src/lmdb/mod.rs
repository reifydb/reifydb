// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Storage;
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};
use std::path::Path;
use std::sync::Arc;

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

pub struct Lmdb {
    pub(crate) env: Arc<Env>,
    pub(crate) db: Database<Bytes, Bytes>,
}

impl Lmdb {
    pub fn new(path: &Path) -> Self {
        let env = unsafe { EnvOpenOptions::new().max_dbs(1).open(path).unwrap() };

        let mut tx = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Bytes>(&mut tx, None).unwrap();
        tx.commit().unwrap();

        Self { env: Arc::new(env), db }
    }
}

impl Storage for Lmdb {}
