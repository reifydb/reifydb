// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::registry::Registry;
use crate::interface::UnversionedStorage;
use std::error::Error;
use std::sync::RwLockWriteGuard;

pub struct OnAfterBootHookContext<'a, US>
where
    US: UnversionedStorage,
{
    pub unversioned: RwLockWriteGuard<'a, US>,
}

impl<'a, US> OnAfterBootHookContext<'a, US>
where
    US: UnversionedStorage,
{
    pub fn new(bypass: RwLockWriteGuard<'a, US>) -> Self {
        Self { unversioned: bypass }
    }
}

pub trait OnAfterBootHook<US>: Send + Sync + 'static
where
    US: UnversionedStorage,
{
    fn on_after_boot(&self, ctx: OnAfterBootHookContext<US>) -> Result<(), Box<dyn Error>>;
}

pub struct LifecycleHookRegistry<US>
where
    US: UnversionedStorage,
{
    after_boot: Registry<dyn OnAfterBootHook<US>>,
}

impl<US> Default for LifecycleHookRegistry<US>
where
    US: UnversionedStorage,
{
    fn default() -> Self {
        Self { after_boot: Registry::default() }
    }
}

impl<US> LifecycleHookRegistry<US>
where
    US: UnversionedStorage,
{
    pub fn after_boot(&self) -> &Registry<dyn OnAfterBootHook<US>> {
        &self.after_boot
    }
}
