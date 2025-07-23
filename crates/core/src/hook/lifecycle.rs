// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::hook::context::HookContext;
use crate::hook::registry::Registry;
use crate::interface::{Transaction, UnversionedStorage, VersionedStorage};
use std::error::Error;

pub trait OnAfterBootHook<VS, US, T>: Send + Sync + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn on_after_boot(&self, ctx: HookContext<VS, US, T>) -> Result<(), Box<dyn Error>>;
}

pub trait OnBeforeBootstrapHook<VS, US, T>: Send + Sync + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn on_before_bootstrap(&self, ctx: &HookContext<VS, US, T>) -> Result<(), Box<dyn Error>>;
}

pub trait OnCreateHook<VS, US, T>: Send + Sync + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn on_create(&self, ctx: &HookContext<VS, US, T>) -> Result<(), Box<dyn Error>>;
}

pub struct LifecycleHookRegistry<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    after_boot: Registry<dyn OnAfterBootHook<VS, US, T>>,
    before_bootstrap: Registry<dyn OnBeforeBootstrapHook<VS, US, T>>,
    on_create: Registry<dyn OnCreateHook<VS, US, T>>,
}

impl<VS, US, T> Default for LifecycleHookRegistry<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn default() -> Self {
        Self {
            after_boot: Registry::default(),
            before_bootstrap: Registry::default(),
            on_create: Registry::default(),
        }
    }
}

impl<VS, US, T> LifecycleHookRegistry<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn after_boot(&self) -> &Registry<dyn OnAfterBootHook<VS, US, T>> {
        &self.after_boot
    }

    pub fn before_bootstrap(&self) -> &Registry<dyn OnBeforeBootstrapHook<VS, US, T>> {
        &self.before_bootstrap
    }

    pub fn on_create(&self) -> &Registry<dyn OnCreateHook<VS, US, T>> {
        &self.on_create
    }
}
