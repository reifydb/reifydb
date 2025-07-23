// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Type;
use reifydb_core::hook::{OnAfterBootHook, OnAfterBootHookContext};
use reifydb_core::interface::UnversionedStorage;
use reifydb_core::interface::{EncodableKey, SystemVersion, SystemVersionKey};
use reifydb_core::row::Layout;
use std::error::Error;

pub struct SystemBootHook {}

impl<US> OnAfterBootHook<US> for SystemBootHook
where
    US: UnversionedStorage,
{
    fn on_after_boot(&self, mut ctx: OnAfterBootHookContext<US>) -> Result<(), Box<dyn Error>> {
        ensure_storage_version(&mut ctx);
        Ok(())
    }
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

fn ensure_storage_version<US>(ctx: &mut OnAfterBootHookContext<US>)
where
    US: UnversionedStorage,
{
    let layout = Layout::new(&[Type::Uint1]);
    let key = SystemVersionKey { version: SystemVersion::Storage }.encode();

    if let None = ctx.unversioned.get(&key).unwrap() {
        let mut row = layout.allocate_row();
        layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
        ctx.unversioned.set(&key, row).unwrap();
    }

    if let Some(unversioned) = ctx.unversioned.get(&key).unwrap() {
        let layout = Layout::new(&[Type::Uint1]);
        let version = layout.get_u8(&unversioned.row, 0);
        assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
    }
}
