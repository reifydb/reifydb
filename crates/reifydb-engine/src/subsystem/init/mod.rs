// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{GetHooks, Transaction};

use crate::{
	Engine,
	subsystem::init::{create::CreateCallback, start::StartCallback},
};

mod create;
pub(crate) mod start;

pub(crate) fn register_system_hooks<T: Transaction>(engine: &Engine<T>) {
	let hooks = engine.get_hooks();

	hooks.register(StartCallback::new(engine.unversioned().clone()));
	hooks.register(CreateCallback::new(engine.clone()));
}
