// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::{EventListener, lifecycle::OnCreateEvent},
	interface::{Engine as EngineInterface, Identity, Params},
};
use reifydb_engine::StandardEngine;
use tracing::error;

pub(crate) struct CreateEventListener {
	engine: StandardEngine,
}

impl CreateEventListener {
	pub(crate) fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}
}

impl EventListener<OnCreateEvent> for CreateEventListener {
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
			error!("Failed to create initial database namespace: {}", e);
		}
	}
}
