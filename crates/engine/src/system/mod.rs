// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use crate::system::start::SystemStartCallback;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, UnversionedTransaction, VersionedTransaction};

pub(crate) mod start;

pub(crate) fn register_system_hooks<VT, UT>(engine: &Engine<VT, UT>)
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine.get_hooks().register::<OnInitHook, SystemStartCallback<UT>>(SystemStartCallback::new(
        engine.unversioned().clone(),
    ));
}
