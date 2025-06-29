// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    GetHooks, Storage, UnversionedRemove, UnversionedSet, UnversionedStorage, VersionedStorage,
};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

#[derive(Clone)]
pub struct Lmdb(Arc<LmdbInner>);

pub struct LmdbInner {
    pub(crate) env: Arc<Env>,
    pub(crate) db: Database<Bytes, Bytes>,
    pub(crate) hooks: Hooks,
}

impl Deref for Lmdb {
    type Target = LmdbInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Lmdb {
    pub fn new(path: &Path) -> Self {
        let env = unsafe { EnvOpenOptions::new().max_dbs(1).open(path).unwrap() };

        let mut tx = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Bytes>(&mut tx, None).unwrap();
        tx.commit().unwrap();

        Self(Arc::new(LmdbInner { env: Arc::new(env), db, hooks: Default::default() }))
    }
}

impl GetHooks for Lmdb {
    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }
}

impl VersionedStorage for Lmdb {}
impl UnversionedStorage for Lmdb {}
impl UnversionedSet for Lmdb {}
impl UnversionedRemove for Lmdb {}
impl Storage for Lmdb {}
