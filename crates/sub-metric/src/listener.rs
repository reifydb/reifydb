// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::{EventListener, metric::RequestExecutedEvent};
use reifydb_runtime::actor::mailbox::ActorRef;

use crate::actor::MetricMsg;

#[derive(Clone)]
pub struct RequestMetricsEventListener {
	actor_ref: ActorRef<MetricMsg>,
}

impl RequestMetricsEventListener {
	pub fn new(actor_ref: ActorRef<MetricMsg>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<RequestExecutedEvent> for RequestMetricsEventListener {
	fn on(&self, event: &RequestExecutedEvent) {
		let _ = self.actor_ref.send(MetricMsg::RequestExecuted(event.clone()));
	}
}
