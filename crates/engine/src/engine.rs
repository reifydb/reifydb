// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_rx, execute_tx};
use crate::system::SystemStartCallback;
use reifydb_core::frame::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::hook::lifecycle::OnStartHook;
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, Transaction, Tx, UnversionedStorage, VersionedStorage,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, RwLockWriteGuard};

pub struct Engine<VS, US, T>(Arc<EngineInner<VS, US, T>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>;

impl<VS, US, T> EngineInterface<VS, US, T> for Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn begin_tx(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin_tx()?)
    }

    fn begin_unversioned_tx(&self) -> RwLockWriteGuard<US> {
        self.transaction.begin_unversioned_tx()
    }

    fn begin_rx(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_rx()?)
    }

    fn tx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
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

    fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
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

    fn hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn transaction(&self) -> &T {
        &self.transaction
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
    hooks: Hooks,
    _phantom: PhantomData<(VS, US)>,
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T, hooks: Hooks) -> crate::Result<Self> {
        let result = Self(Arc::new(EngineInner { transaction, hooks, _phantom: PhantomData }));
        result.setup_hooks();
        Ok(result)
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn setup_hooks(&self) {
        self.hooks.register::<OnStartHook, SystemStartCallback<VS, US, T>>(
            SystemStartCallback::new(self.transaction.clone()),
        );
    }
}

impl<VS, US, T> Engine<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
}
