// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	actors::metric::MetricMessage,
	event::{EventListener, metric::RequestExecutedEvent},
};
use reifydb_runtime::actor::mailbox::ActorRef;

#[derive(Clone)]
pub struct RequestMetricsEventListener {
	actor_ref: ActorRef<MetricMessage>,
}

impl RequestMetricsEventListener {
	pub fn new(actor_ref: ActorRef<MetricMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<RequestExecutedEvent> for RequestMetricsEventListener {
	fn on(&self, event: &RequestExecutedEvent) {
		let _ = self.actor_ref.send(MetricMessage::RequestExecuted(event.clone()));
	}
}
