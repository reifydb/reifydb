// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::hook::lifecycle::LifecycleHookRegistry;
use crate::hook::transaction::TransactionHookRegistry;
use crate::interface::{Engine, Transaction, UnversionedStorage, VersionedStorage};
use std::ops::Deref;
use std::sync::Arc;

mod context;
mod lifecycle;
mod registry;
mod transaction;

pub use context::*;
pub use lifecycle::{OnAfterBootHook, OnBeforeBootstrapHook, OnCreateHook};
pub use transaction::{PostCommitHook, PreCommitHook};

#[derive(Clone)]
pub struct Hooks<VS, US, T>(Arc<HooksInner<VS, US, T>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>;

pub struct HooksInner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    lifecycle: LifecycleHookRegistry<VS, US, T>,
    transaction: TransactionHookRegistry,
}

impl<VS, US, T> Deref for Hooks<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    type Target = HooksInner<VS, US, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS, US, T> Default for Hooks<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn default() -> Self {
        Self(Arc::new(HooksInner {
            lifecycle: LifecycleHookRegistry::default(),
            transaction: Default::default(),
        }))
    }
}

impl<VS, US, T> Hooks<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn lifecycle(&self) -> &LifecycleHookRegistry<VS, US, T> {
        &self.lifecycle
    }

    pub fn transaction(&self) -> &TransactionHookRegistry {
        &self.transaction
    }
}
