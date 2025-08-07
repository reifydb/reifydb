// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use crate::subsystem::init::start::StartCallback;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};
use crate::subsystem::init::create::CreateCallback;

mod create;
pub(crate) mod start;

pub(crate) fn register_system_hooks<VT, UT>(engine: &Engine<VT, UT>)
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    let hooks = engine.get_hooks();

    hooks.register(StartCallback::new(engine.unversioned().clone()));
    hooks.register(CreateCallback::new(engine.clone()));
}
