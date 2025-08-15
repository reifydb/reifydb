// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::{BoxedHookIter, Callback, lifecycle::OnCreateHook},
	interface::{Engine as EngineInterface, Identity, Params, Transaction},
	return_hooks,
};
use reifydb_engine::StandardEngine;

pub(crate) struct CreateCallback<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> CreateCallback<T> {
	pub(crate) fn new(engine: StandardEngine<T>) -> Self {
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
			Params::None,
		)?;

		return_hooks!()
	}
}
