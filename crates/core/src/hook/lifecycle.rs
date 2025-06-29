// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::registry::Registry;
use std::error::Error;

pub struct OnAfterBootHookContext {
}

pub trait OnAfterBootHook: Send + Sync + 'static {
    fn on_after_boot(&self, ctx: OnAfterBootHookContext) -> Result<(), Box<dyn Error>>;
}

#[derive(Default)]
pub struct LifecycleHookRegistry {
    after_boot: Registry<dyn OnAfterBootHook>,
}

impl LifecycleHookRegistry {
    pub fn after_boot(&self) -> &Registry<dyn OnAfterBootHook> {
        &self.after_boot
    }
}
