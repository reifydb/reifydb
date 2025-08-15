// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::{lifecycle::OnCreateHook, BoxedHookIter, Callback},
	interface::{Engine as EngineInterface, Identity, Transaction},
	return_hooks,
};
use reifydb_engine::Engine;

pub(crate) struct CreateCallback<T: Transaction> {
	engine: Engine<T>,
}

impl<T: Transaction> CreateCallback<T> {
	pub(crate) fn new(engine: Engine<T>) -> Self {
		Self {
			engine,
		}
	}
}

impl<T: Transaction> Callback<OnCreateHook> for CreateCallback<T> {
	fn on(&self, _hook: &OnCreateHook) -> crate::Result<BoxedHookIter> {
		self.engine.command_as(
			&Identity::root(),
			r#"

create schema reifydb;

create table reifydb.flows{
    id: int8 auto increment,
    data: blob
};

"#,
			reifydb_core::interface::Params::None,
		)?;
		return_hooks!()
	}
}
