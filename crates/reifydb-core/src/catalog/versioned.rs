// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use core::ops::Deref;
use core::sync::atomic::{AtomicU8, Ordering};
use std::fmt::Debug;

use crossbeam_skiplist::SkipMap;

use crate::{
    Version,
    interface::{SchemaDef, TableDef, ViewDef},
};

const UNINITIALIZED: u8 = 0;
const LOCKED: u8 = 1;
const UNLOCKED: u8 = 2;

#[derive(Debug)]
pub struct VersionedSchemaDef {
    pub(crate) op: AtomicU8,
    schemas: SkipMap<Version, Option<SchemaDef>>,
}

impl VersionedSchemaDef {
    pub(crate) fn new() -> Self {
        Self {
            op: AtomicU8::new(UNINITIALIZED),
            schemas: SkipMap::new(),
        }
    }

    pub(crate) fn lock(&self) {
        let mut current = UNLOCKED;
        // Spin lock is ok here because the lock is expected to be held
        // for a very short time. and it is hardly contended.
        loop {
            match self.op.compare_exchange_weak(
                current,
                LOCKED,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(old) => {
                    // If the current state is
                    // uninitialized, we can directly
                    // return. as we are based on
                    // SkipMap, let it to handle concurrent
                    // write is enough.
                    if old == UNINITIALIZED {
                        return;
                    }

                    current = old;
                }
            }
        }
    }

    pub(crate) fn unlock(&self) {
        self.op.store(UNLOCKED, Ordering::Release);
    }
}

impl Deref for VersionedSchemaDef {
    type Target = SkipMap<Version, Option<SchemaDef>>;

    fn deref(&self) -> &Self::Target {
        &self.schemas
    }
}

#[derive(Debug)]
pub struct VersionedTableDef {
    pub(crate) op: AtomicU8,
    tables: SkipMap<Version, Option<TableDef>>,
}

impl VersionedTableDef {
    pub(crate) fn new() -> Self {
        Self {
            op: AtomicU8::new(UNINITIALIZED),
            tables: SkipMap::new(),
        }
    }

    pub(crate) fn lock(&self) {
        let mut current = UNLOCKED;
        loop {
            match self.op.compare_exchange_weak(
                current,
                LOCKED,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(old) => {
                    if old == UNINITIALIZED {
                        return;
                    }
                    current = old;
                }
            }
        }
    }

    pub(crate) fn unlock(&self) {
        self.op.store(UNLOCKED, Ordering::Release);
    }
}

impl Deref for VersionedTableDef {
    type Target = SkipMap<Version, Option<TableDef>>;

    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

#[derive(Debug)]
pub struct VersionedViewDef {
    pub(crate) op: AtomicU8,
    views: SkipMap<Version, Option<ViewDef>>,
}

impl VersionedViewDef {
    pub(crate) fn new() -> Self {
        Self {
            op: AtomicU8::new(UNINITIALIZED),
            views: SkipMap::new(),
        }
    }

    pub(crate) fn lock(&self) {
        let mut current = UNLOCKED;
        loop {
            match self.op.compare_exchange_weak(
                current,
                LOCKED,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(old) => {
                    if old == UNINITIALIZED {
                        return;
                    }
                    current = old;
                }
            }
        }
    }

    pub(crate) fn unlock(&self) {
        self.op.store(UNLOCKED, Ordering::Release);
    }
}

impl Deref for VersionedViewDef {
    type Target = SkipMap<Version, Option<ViewDef>>;

    fn deref(&self) -> &Self::Target {
        &self.views
    }
}