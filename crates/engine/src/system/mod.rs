// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use crate::system::start::SystemStartCallback;
use reifydb_core::hook::lifecycle::OnInitHook;
use reifydb_core::interface::{
    GetHooks, UnversionedTransaction, VersionedTransaction, UnversionedStorage, VersionedStorage,
};

pub(crate) mod start;

pub(crate) fn register_system_hooks<VS, US, T, UT>(engine: &Engine<VS, US, T, UT>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    engine.get_hooks().register::<OnInitHook, SystemStartCallback<VS, US, UT>>(
        SystemStartCallback::new(engine.unversioned().clone()),
    );
}
