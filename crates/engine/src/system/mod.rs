// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Type;
use reifydb_core::hook::{HookContext, OnAfterBootHook};
use reifydb_core::interface::{EncodableKey, SystemVersion, SystemVersionKey};
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_core::row::Layout;
use std::error::Error;

pub struct SystemBootHook {}

impl<VS, US, T> OnAfterBootHook<VS, US, T> for SystemBootHook
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn on_after_boot(&self, mut ctx: HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> {
        ensure_storage_version(&mut ctx);
        Ok(())
    }
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

fn ensure_storage_version<VS, US, T>(ctx: &mut HookContext<VS, US, T>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    let layout = Layout::new(&[Type::Uint1]);
    let key = SystemVersionKey { version: SystemVersion::Storage }.encode();

    // if let None = ctx.unversioned.get(&key).unwrap() {
    //     let mut row = layout.allocate_row();
    //     layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
    //     ctx.unversioned.set(&key, row).unwrap();
    // }
    //
    // if let Some(unversioned) = ctx.unversioned.get(&key).unwrap() {
    //     let layout = Layout::new(&[Type::Uint1]);
    //     let version = layout.get_u8(&unversioned.row, 0);
    //     assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
    // }

    todo!()
}
