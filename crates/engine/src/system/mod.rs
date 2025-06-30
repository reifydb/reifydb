// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::hook::{OnAfterBootHook, OnAfterBootHookContext};
use reifydb_core::interface::UnversionedStorage;
use std::error::Error;

pub struct SystemBootHook {}

impl<US> OnAfterBootHook<US> for SystemBootHook
where
    US: UnversionedStorage,
{
    fn on_after_boot(&self, mut ctx: OnAfterBootHookContext<US>) -> Result<(), Box<dyn Error>> {
        // if let None =
        //     ctx.unversioned.get(&SystemVersionKey { version: SystemVersion::Storage }.encode())?
        // {
        //     println!("set storage version to 1");
        // }

        println!("System booted.");
        Ok(())
    }
}
