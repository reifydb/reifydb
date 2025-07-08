// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{execute_rx, execute_tx};
use crate::frame::Frame;
use crate::system::SystemBootHook;
use crate::view;
use reifydb_core::hook::{Hooks, OnAfterBootHookContext};
use reifydb_core::interface::{Principal, Transaction, Tx, UnversionedStorage, VersionedStorage};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>>(
    Arc<EngineInner<VS, US, T>>,
);

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

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Deref
    for Engine<VS, US, T>
{
    type Target = EngineInner<VS, US, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> {
    transaction: T,
    hooks: Hooks<US>,
    deferred_view: Arc<view::deferred::Engine<VS, US>>,
    _marker: PhantomData<(VS, US)>,
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Engine<VS, US, T> {
    pub fn new(transaction: T) -> crate::Result<Self> {
        let storage = transaction.versioned();
        let deferred_view = view::deferred::Engine::new(storage);
        let hooks = transaction.hooks();
        let result =
            Self(Arc::new(EngineInner { transaction, hooks, deferred_view, _marker: PhantomData }));
        result.setup_hooks();
        result.boot()?;
        Ok(result)
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Engine<VS, US, T> {
    pub fn boot(&self) -> crate::Result<()> {
        self.hooks
            .lifecycle()
            .after_boot()
            .for_each(|hook| {
                Ok(hook.on_after_boot(OnAfterBootHookContext::new(
                    self.transaction.begin_unversioned_tx(),
                ))?)
            })
            .unwrap(); // FIXME
        Ok(())
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Engine<VS, US, T> {
    pub fn setup_hooks(&self) {
        self.hooks.lifecycle().after_boot().register(Arc::new(SystemBootHook {}));
        self.hooks.transaction().post_commit().register(self.deferred_view.clone());
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> Engine<VS, US, T> {
    pub fn begin_tx(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin_tx()?)
    }

    pub fn begin_rx(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_rx()?)
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
