// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{execute_rx, execute_tx};
use crate::system::register_system_hooks;
use reifydb_core::Frame;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    ActiveReadTransaction, ActiveWriteTransaction, Engine as EngineInterface, GetHooks, Principal,
    UnversionedTransaction, VersionedTransaction, VersionedWriteTransaction,
};
use reifydb_rql::ast;
use reifydb_rql::plan::plan;
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
    fn begin_write(&self) -> crate::Result<ActiveWriteTransaction<VT, UT>> {
        Ok(ActiveWriteTransaction::new(self.versioned.begin_write()?, self.unversioned.clone()))
    }

    fn begin_read(&self) -> crate::Result<ActiveReadTransaction<VT, UT>> {
        Ok(ActiveReadTransaction::new(self.versioned.begin_read()?, self.unversioned.clone()))
    }

    fn write_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut atx = self.begin_write()?;

        for statement in statements {
            if let Some(plan) = plan(&mut atx, statement)? {
                let er = execute_tx(&mut atx, plan)?;
                result.push(er);
            }
        }

        atx.commit()?;

        Ok(result.into_iter().map(Frame::from).collect())
    }

    fn read_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>> {
        let mut result = vec![];
        let statements = ast::parse(rql)?;

        let mut rx = self.begin_read()?;
        for statement in statements {
            if let Some(plan) = plan(&mut rx, statement)? {
                let er = execute_rx::<VT, UT>(&mut rx, plan)?;
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
    _phantom: PhantomData<(VT, UT)>,
}

impl<VT, UT> Engine<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(versioned: VT, unversioned: UT, hooks: Hooks) -> crate::Result<Self> {
        let result =
            Self(Arc::new(EngineInner { versioned, unversioned, hooks, _phantom: PhantomData }));
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
