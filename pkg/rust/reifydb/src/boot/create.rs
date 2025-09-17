// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::{Engine as EngineInterface, Identity, Params, Transaction},
	log_error,
};
use reifydb_engine::StandardEngine;

pub(crate) struct CreateEventListener<T: Transaction> {
	engine: StandardEngine<T>,
}

impl<T: Transaction> CreateEventListener<T> {
	pub(crate) fn new(engine: StandardEngine<T>) -> Self {
		Self {
			engine,
		}
	}
}

impl<T: Transaction> EventListener<OnCreateEvent> for CreateEventListener<T> {
	fn on(&self, _event: &OnCreateEvent) {
		if let Err(e) = self.engine.command_as(
			&Identity::root(),
			r#"

create namespace reifydb;

create table reifydb.flows{
    id: int8 auto increment,
    data: blob
};

"#,
			Params::None,
		) {
			log_error!("Failed to create initial database namespace: {}", e);
		}
	}
}
