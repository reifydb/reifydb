// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::hook::{OnAfterBootHook, OnAfterBootHookContext};
use std::error::Error;

pub struct SystemBootHook {}

impl OnAfterBootHook for SystemBootHook {
    fn on_after_boot(&self, ctx: OnAfterBootHookContext) -> Result<(), Box<dyn Error>> {
        println!("System booted.");
        Ok(())
    }
}
