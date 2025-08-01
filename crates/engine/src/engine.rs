// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_rx, execute_tx};
use crate::system::register_system_hooks;
use reifydb_core::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    Engine as EngineInterface, GetHooks, Principal, UnversionedStorage, UnversionedTransaction,
    VersionedStorage, VersionedTransaction, VersionedWriteTransaction, ActiveReadTransaction,
    ActiveWriteTransaction,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VS, US, T, UT>(Arc<EngineInner<VS, US, T, UT>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction;

impl<VS, US, T, UT> GetHooks for Engine<VS, US, T, UT>
where
    T: VersionedTransaction<VS, US>,
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
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn begin_write(&self) -> crate::Result<T::Write> {
        Ok(self.transaction.begin_write()?)
    }

    fn begin_read(&self) -> crate::Result<T::Read> {
        Ok(self.transaction.begin_read()?)
    }

    fn write_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut tx = self.begin_write()?;

        for statement in statements {
            if let Some(plan) = plan(&mut tx, statement)? {
                let er = execute_tx(&mut tx, plan)?;
                result.push(er);
            }
        }

        tx.commit()?;

        Ok(result.into_iter().map(Frame::from).collect())
    }

    fn read_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut rx = self.begin_read()?;
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
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn transaction(&self) -> &T {
        &self.transaction
    }

    pub fn unversioned(&self) -> &UT {
        &self.unversioned
    }

    /// Begin a read active transaction
    pub fn begin_active_read(&self) -> crate::Result<ActiveReadTransaction<VS, US, T, UT>> {
        let read_tx = self.begin_read()?;
        Ok(ActiveReadTransaction::new(read_tx, self.unversioned.clone()))
    }

    /// Begin a write active transaction
    pub fn begin_active_write(&self) -> crate::Result<ActiveWriteTransaction<VS, US, T, UT>> {
        let write_tx = self.begin_write()?;
        Ok(ActiveWriteTransaction::new(write_tx, self.unversioned.clone()))
    }
}

impl<VS, US, T, UT> Clone for Engine<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
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
    T: VersionedTransaction<VS, US>,
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
    T: VersionedTransaction<VS, US>,
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
    T: VersionedTransaction<VS, US>,
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
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn setup_hooks(&self) -> crate::Result<()> {
        register_system_hooks(&self);
        Ok(())
    }
}
