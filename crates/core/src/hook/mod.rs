// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::lifecycle::LifecycleHookRegistry;
use crate::hook::transaction::TransactionHookRegistry;
use std::ops::Deref;
use std::sync::Arc;
pub use lifecycle::{OnAfterBootHook, OnAfterBootHookContext};
pub use transaction::{PostCommitHook, PreCommitHook};

mod lifecycle;
mod registry;
mod transaction;

#[derive(Clone)]
pub struct Hooks(Arc<HooksInner>);

pub struct HooksInner {
    lifecycle: LifecycleHookRegistry,
    transaction: TransactionHookRegistry,
}

impl Deref for Hooks {
    type Target = HooksInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Hooks {
    fn default() -> Self {
        Self(Arc::new(HooksInner {
            lifecycle: LifecycleHookRegistry::default(),
            transaction: Default::default(),
        }))
    }
}

impl Hooks {
    pub fn lifecycle(&self) -> &LifecycleHookRegistry {
        &self.lifecycle
    }

    pub fn transaction(&self) -> &TransactionHookRegistry {
        &self.transaction
    }
}
