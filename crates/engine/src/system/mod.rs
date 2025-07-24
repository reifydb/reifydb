// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::hook::lifecycle::OnStartHook;
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{EncodableKey, SystemVersion, SystemVersionKey};
use reifydb_core::interface::{
    Engine as EngineInterface, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_core::row::Layout;
use reifydb_core::{Type, return_hooks};

pub struct SystemStartCallback {}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl<VS, US, T, E> Callback<OnStartHook<VS, US, T, E>> for SystemStartCallback
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    fn on(&self, hook: &OnStartHook<VS, US, T, E>) -> Result<BoxedHookIter, reifydb_core::Error> {
        let layout = Layout::new(&[Type::Uint1]);
        let key = SystemVersionKey { version: SystemVersion::Storage }.encode();

        let mut unversioned = hook.engine.begin_unversioned_tx();

        if let None = unversioned.get(&key).unwrap() {
            let mut row = layout.allocate_row();
            layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
            unversioned.set(&key, row).unwrap();
        }

        if let Some(unversioned) = unversioned.get(&key).unwrap() {
            let layout = Layout::new(&[Type::Uint1]);
            let version = layout.get_u8(&unversioned.row, 0);
            assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
        }
        return_hooks!()
    }
}
