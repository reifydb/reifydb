// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_rx, execute_tx};
use crate::system::register_system_hooks;
use reifydb_core::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, GetHooks, UnversionedTransaction, Principal, Transaction, Tx,
    UnversionedStorage, VersionedStorage,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, MutexGuard};

pub struct Engine<VS, US, T, UT>(Arc<EngineInner<VS, US, T, UT>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction;

impl<VS, US, T, UT> GetHooks for Engine<VS, US, T, UT>
where
    T: Transaction<VS, US>,
    US: UnversionedStorage,
    VS: VersionedStorage,
    UT: UnversionedTransaction,
{
    fn get_hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<VS, US, T, UT> EngineInterface<VS, US, T, UT> for Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn begin_tx(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin_tx()?)
    }

    fn begin_unversioned(&self) -> MutexGuard<US> {
        self.transaction.begin_unversioned()
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

        Ok(result.into_iter().map(Frame::from).collect())
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

        Ok(result.into_iter().map(Frame::from).collect())
    }
}

impl<VS, US, T, UT> Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn transaction(&self) -> &T {
        &self.transaction
    }

    pub fn unversioned(&self) -> &UT {
        &self.unversioned
    }
}

impl<VS, US, T, UT> Clone for Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VS, US, T, UT> Deref for Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    type Target = EngineInner<VS, US, T, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    transaction: T,
    unversioned: UT,
    hooks: Hooks,
    _phantom: PhantomData<(VS, US)>,
}

impl<VS, US, T, UT> Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn new(transaction: T, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        let result =
            Self(Arc::new(EngineInner { transaction, unversioned, hooks, _phantom: PhantomData }));
        result.setup_hooks()?;
        Ok(result)
    }
}

impl<VS, US, T, UT> Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn setup_hooks(&self) -> crate::Result<()> {
        register_system_hooks(&self);
        Ok(())
    }
}
