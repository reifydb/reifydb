// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_command_plan, execute_query_plan};
use crate::system::register_system_hooks;
use reifydb_core::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, Command, Engine as EngineInterface,
    ExecuteCommand, ExecuteQuery, GetHooks, Params, Principal, Query, UnversionedTransaction,
    VersionedCommandTransaction, VersionedTransaction,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
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
    fn execute_command<'a>(
        &'a self,
        atx: &mut ActiveCommandTransaction<VT, UT>,
        cmd: Command<'a>,
    ) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(cmd.rql)?;

        for statement in statements {
            if let Some(plan) = plan(atx, statement)? {
                let er = execute_command_plan(atx, plan, cmd.params.clone())?;
                result.push(er);
            }
        }

        Ok(result.into_iter().map(Frame::from).collect())
    }
}

impl<VT, UT> ExecuteQuery<VT, UT> for Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn execute_query<'a>(
        &'a self,
        atx: &mut ActiveQueryTransaction<VT, UT>,
        qry: Query<'a>,
    ) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(qry.rql)?;

        for statement in statements {
            if let Some(plan) = plan(atx, statement)? {
                let er = execute_query_plan::<VT, UT>(atx, plan, qry.params.clone())?;
                result.push(er);
            }
        }

        Ok(result.into_iter().map(Frame::from).collect())
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
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        let result = Self(Arc::new(EngineInner { versioned, unversioned, hooks }));
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
