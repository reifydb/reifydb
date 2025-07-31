// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use crate::system::start::SystemStartCallback;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{GetHooks, Transaction, UnversionedStorage, VersionedStorage};

pub(crate) mod start;

pub(crate) fn register_system_hooks<VS, US, T>(engine: &Engine<VS, US, T>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine.get_hooks().register::<OnInitHook, SystemStartCallback<VS, US, T>>(
        SystemStartCallback::new(engine.transaction().clone()),
    );
}
