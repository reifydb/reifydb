// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_rx, execute_tx};
use crate::frame::Frame;
use crate::system::SystemBootHook;
use crate::view;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as _, Principal, Transaction, Tx, UnversionedStorage, VersionedStorage,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VS, US, T>(Arc<EngineInner<VS, US, T>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>;

impl<VS, US, T> reifydb_core::interface::Engine<VS, US, T> for Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn begin_tx(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin_tx()?)
    }

    fn begin_rx(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_rx()?)
    }
}

impl<VS, US, T> Clone for Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VS, US, T> Deref for Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    type Target = EngineInner<VS, US, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    transaction: T,
    hooks: Hooks<VS, US, T>,
    deferred_view: Arc<view::deferred::Engine<VS, US>>,
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T) -> crate::Result<Self> {
        let storage = transaction.versioned();
        let deferred_view = view::deferred::Engine::new(storage);
        let hooks = transaction.hooks();
        let result = Self(Arc::new(EngineInner { transaction, hooks, deferred_view }));
        result.setup_hooks();
        result.boot()?;
        Ok(result)
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn boot(&self) -> crate::Result<()> {
        self.hooks
            .lifecycle()
            .after_boot()
            .for_each(|hook| {
                todo!()
                // Ok(hook.on_after_boot(OnAfterBootHookContext::new(
                //     self.transaction.begin_unversioned_tx(),
                // ))?)
            })
            .unwrap(); // FIXME
        Ok(())
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn setup_hooks(&self) {
        self.hooks.lifecycle().after_boot().register(Arc::new(SystemBootHook {}));
        self.hooks.transaction().post_commit().register(self.deferred_view.clone());
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn hooks(&self) -> &Hooks<VS, US, T> {
        &self.hooks
    }

    pub fn tx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut tx = self.begin_tx()?;

        for statement in statements {
            if let Some(plan) = plan(&mut tx, statement)? {
                let er = execute_tx(&mut tx, plan)?;
                result.push(er);
            }
        }

        tx.commit()?;

        Ok(result)
    }

    pub fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut rx = self.begin_rx()?;
        for statement in statements {
            if let Some(plan) = plan(&mut rx, statement)? {
                let er = execute_rx::<VS, US>(&mut rx, plan)?;
                result.push(er);
            }
        }

        Ok(result)
    }
}
