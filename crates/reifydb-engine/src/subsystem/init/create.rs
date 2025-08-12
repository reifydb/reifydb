// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use reifydb_core::hook::lifecycle::OnCreateHook;
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, Transaction,
};
use reifydb_core::return_hooks;

pub(crate) struct CreateCallback<T>
where
    T: Transaction,
{
    engine: Engine<T>,
}

impl<T> CreateCallback<T>
where
    T: Transaction,
{
    pub(crate) fn new(engine: Engine<T>) -> Self {
        Self { engine }
    }
}

impl<T> Callback<OnCreateHook> for CreateCallback<T>
where
    T: Transaction,
{
    fn on(&self, _hook: &OnCreateHook) -> crate::Result<BoxedHookIter> {
        self.engine.command_as(
            &Principal::root(),
            r#"

create schema reifydb;

create table reifydb.flows{
    id: int8 auto increment,
    data: blob
};

"#,
            reifydb_core::interface::Params::None,
        )?;
        return_hooks!()
    }
}
