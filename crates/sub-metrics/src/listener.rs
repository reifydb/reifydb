// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	actors::metrics::MetricsMessage,
	event::{
		EventListener,
		metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent},
	},
};
use reifydb_runtime::actor::mailbox::ActorRef;

#[derive(Clone)]
pub struct RequestMetricsEventListener {
	actor_ref: ActorRef<MetricsMessage>,
}

impl RequestMetricsEventListener {
	pub fn new(actor_ref: ActorRef<MetricsMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<RequestExecutedEvent> for RequestMetricsEventListener {
	fn on(&self, event: &RequestExecutedEvent) {
		let _ = self.actor_ref.send(MetricsMessage::RequestExecuted(event.clone()));
	}
}

#[derive(Clone)]
pub struct MultiCommittedListener {
	actor_ref: ActorRef<MetricsMessage>,
}

impl MultiCommittedListener {
	pub fn new(actor_ref: ActorRef<MetricsMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<MultiCommittedEvent> for MultiCommittedListener {
	fn on(&self, event: &MultiCommittedEvent) {
		let _ = self.actor_ref.send(MetricsMessage::MultiCommitted(event.clone()));
	}
}

#[derive(Clone)]
pub struct CdcWrittenListener {
	actor_ref: ActorRef<MetricsMessage>,
}

impl CdcWrittenListener {
	pub fn new(actor_ref: ActorRef<MetricsMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<CdcWrittenEvent> for CdcWrittenListener {
	fn on(&self, event: &CdcWrittenEvent) {
		if !event.entries().is_empty() {
			let _ = self.actor_ref.send(MetricsMessage::CdcWritten(event.clone()));
		}
	}
}

#[derive(Clone)]
pub struct CdcEvictedListener {
	actor_ref: ActorRef<MetricsMessage>,
}

impl CdcEvictedListener {
	pub fn new(actor_ref: ActorRef<MetricsMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<CdcEvictedEvent> for CdcEvictedListener {
	fn on(&self, event: &CdcEvictedEvent) {
		if !event.entries().is_empty() {
			let _ = self.actor_ref.send(MetricsMessage::CdcEvicted(event.clone()));
		}
	}
}
