// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::Executor;
use crate::function::{Functions, math};
use crate::system::register_system_hooks;
use reifydb_core::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, Command, Engine as EngineInterface,
    ExecuteCommand, ExecuteQuery, GetHooks, Params, Principal, Query, UnversionedTransaction,
    VersionedCommandTransaction, VersionedTransaction,
};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub struct Engine<VT, UT>(Arc<EngineInner<VT, UT>>)
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction;

impl<VT, UT> GetHooks for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn get_hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<VT, UT> EngineInterface<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn begin_command(&self) -> crate::Result<ActiveCommandTransaction<VT, UT>> {
        Ok(ActiveCommandTransaction::new(self.versioned.begin_command()?, self.unversioned.clone()))
    }

    fn begin_query(&self) -> crate::Result<ActiveQueryTransaction<VT, UT>> {
        Ok(ActiveQueryTransaction::new(self.versioned.begin_query()?, self.unversioned.clone()))
    }

    fn command_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: Params,
    ) -> crate::Result<Vec<Frame>> {
        let mut atx = self.begin_command()?;
        let result = self.execute_command(&mut atx, Command { rql, params, principal })?;
        atx.commit()?;
        Ok(result)
    }

    fn query_as(
        &self,
        principal: &Principal,
        rql: &str,
        params: Params,
    ) -> crate::Result<Vec<Frame>> {
        let mut atx = self.begin_query()?;
        let result = self.execute_query(&mut atx, Query { rql, params, principal })?;
        Ok(result)
    }
}

impl<VT, UT> ExecuteCommand<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn execute_command<'a>(
        &'a self,
        atx: &mut ActiveCommandTransaction<VT, UT>,
        cmd: Command<'a>,
    ) -> crate::Result<Vec<Frame>> {
        self.executor.execute_command(atx, cmd)
    }
}

impl<VT, UT> ExecuteQuery<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn execute_query<'a>(
        &'a self,
        atx: &mut ActiveQueryTransaction<VT, UT>,
        qry: Query<'a>,
    ) -> crate::Result<Vec<Frame>> {
        self.executor.execute_query(atx, qry)
    }
}

impl<VT, UT> Clone for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VT, UT> Deref for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    type Target = EngineInner<VT, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    versioned: VT,
    unversioned: UT,
    hooks: Hooks,
    executor: Executor<VT, UT>,
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        let result = Self(Arc::new(EngineInner {
            versioned,
            unversioned,
            hooks,
            executor: Executor {
                functions: Functions::builder()
                    .register_aggregate("sum", math::aggregate::Sum::new)
                    .register_aggregate("min", math::aggregate::Min::new)
                    .register_aggregate("max", math::aggregate::Max::new)
                    .register_aggregate("avg", math::aggregate::Avg::new)
                    .register_scalar("abs", math::scalar::Abs::new)
                    .register_scalar("avg", math::scalar::Avg::new)
                    .build(),
                _phantom: PhantomData,
            },
        }));
        result.setup_hooks()?;
        Ok(result)
    }

    pub fn unversioned(&self) -> &UT {
        &self.unversioned
    }
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn setup_hooks(&self) -> crate::Result<()> {
        register_system_hooks(&self);
        Ok(())
    }
}
