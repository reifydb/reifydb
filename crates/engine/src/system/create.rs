// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use reifydb_core::hook::lifecycle::OnCreateHook;
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, UnversionedTransaction, VersionedTransaction,
};
use reifydb_core::return_hooks;

pub(crate) struct CreateCallback<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: Engine<VT, UT>,
}

impl<VT, UT> CreateCallback<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: Engine<VT, UT>) -> Self {
        Self { engine }
    }
}

impl<VT, UT> Callback<OnCreateHook> for CreateCallback<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn on(&self, _hook: &OnCreateHook) -> crate::Result<BoxedHookIter> {
        self.engine.write_as(
            &Principal::root(),
            r#"

create schema reifydb;

create table reifydb.flows{
    id: int8 auto increment,
    data: blob
};

"#,
        )?;
        return_hooks!()
    }
}
