// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use futures_util::TryStreamExt;
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

#[async_trait]
impl EventListener<OnCreateEvent> for CreateEventListener {
	async fn on(&self, _event: &OnCreateEvent) {
		let stream = self.engine.command_as(
			&Identity::root(),
			r#"

create namespace reifydb;

create table reifydb.flows{
    id: int8 auto increment,
    data: blob
};

"#,
			Params::None,
		);

		if let Err(e) = stream.try_collect::<Vec<_>>().await {
			error!("Failed to create initial database namespace: {:?}", e);
		}
	}
}
