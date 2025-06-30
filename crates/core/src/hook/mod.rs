// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::lifecycle::LifecycleHookRegistry;
use crate::hook::transaction::TransactionHookRegistry;
use crate::interface::UnversionedStorage;
pub use lifecycle::{OnAfterBootHook, OnAfterBootHookContext};
use std::ops::Deref;
use std::sync::Arc;
pub use transaction::{PostCommitHook, PreCommitHook};

mod lifecycle;
mod registry;
mod transaction;

#[derive(Clone)]
pub struct Hooks<US>(Arc<HooksInner<US>>)
where
    US: UnversionedStorage;

pub struct HooksInner<US>
where
    US: UnversionedStorage,
{
    lifecycle: LifecycleHookRegistry<US>,
    transaction: TransactionHookRegistry,
}

impl<US> Deref for Hooks<US>
where
    US: UnversionedStorage,
{
    type Target = HooksInner<US>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<US> Default for Hooks<US>
where
    US: UnversionedStorage,
{
    fn default() -> Self {
        Self(Arc::new(HooksInner {
            lifecycle: LifecycleHookRegistry::default(),
            transaction: Default::default(),
        }))
    }
}

impl<US> Hooks<US>
where
    US: UnversionedStorage,
{
    pub fn lifecycle(&self) -> &LifecycleHookRegistry<US> {
        &self.lifecycle
    }

    pub fn transaction(&self) -> &TransactionHookRegistry {
        &self.transaction
    }
}
