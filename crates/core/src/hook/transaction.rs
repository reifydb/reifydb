// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::delta::Delta;
use crate::hook::registry::Registry;
use crate::{AsyncCowVec, Version};
use std::error::Error;

pub trait PreCommitHook: Send + Sync + 'static {
    // if this hook fails, it rolls back the transaction
    fn on_pre_commit(
        &self,
        deltas: AsyncCowVec<Delta>,
        version: Version,
    ) -> Result<(), Box<dyn Error>>;
}

pub trait PostCommitHook: Send + Sync + 'static {
    fn on_post_commit(&self, deltas: AsyncCowVec<Delta>, version: Version);
}

pub struct TransactionHookRegistry {
    pre_commit: Registry<dyn PreCommitHook>,
    post_commit: Registry<dyn PostCommitHook>,
}

impl Default for TransactionHookRegistry {
    fn default() -> Self {
        Self { pre_commit: Default::default(), post_commit: Default::default() }
    }
}

impl TransactionHookRegistry {
    pub fn pre_commit(&self) -> &Registry<dyn PreCommitHook> {
        &self.pre_commit
    }

    pub fn post_commit(&self) -> &Registry<dyn PostCommitHook> {
        &self.post_commit
    }
}
