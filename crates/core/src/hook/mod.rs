// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::transaction::TransactionHookRegistry;
use std::ops::Deref;
use std::sync::Arc;
pub use transaction::{PostCommitHook, PreCommitHook};

mod registry;
mod transaction;

#[derive(Clone)]
pub struct Hooks(Arc<HooksInner>);

pub struct HooksInner {
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
        Self(Arc::new(HooksInner { transaction: Default::default() }))
    }
}

impl Hooks {
    pub fn transaction(&self) -> &TransactionHookRegistry {
        &self.transaction
    }
}
