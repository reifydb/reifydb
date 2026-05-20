// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	actors::metric::MetricMessage,
	event::{
		EventListener,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, ProfilerSnapshotEvent,
			RequestExecutedEvent,
		},
	},
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

#[derive(Clone)]
pub struct MultiCommittedListener {
	actor_ref: ActorRef<MetricMessage>,
}

impl MultiCommittedListener {
	pub fn new(actor_ref: ActorRef<MetricMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<MultiCommittedEvent> for MultiCommittedListener {
	fn on(&self, event: &MultiCommittedEvent) {
		let _ = self.actor_ref.send(MetricMessage::MultiCommitted(event.clone()));
	}
}

#[derive(Clone)]
pub struct CdcWrittenListener {
	actor_ref: ActorRef<MetricMessage>,
}

impl CdcWrittenListener {
	pub fn new(actor_ref: ActorRef<MetricMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<CdcWrittenEvent> for CdcWrittenListener {
	fn on(&self, event: &CdcWrittenEvent) {
		if !event.entries().is_empty() {
			let _ = self.actor_ref.send(MetricMessage::CdcWritten(event.clone()));
		}
	}
}

#[derive(Clone)]
pub struct CdcEvictedListener {
	actor_ref: ActorRef<MetricMessage>,
}

impl CdcEvictedListener {
	pub fn new(actor_ref: ActorRef<MetricMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<CdcEvictedEvent> for CdcEvictedListener {
	fn on(&self, event: &CdcEvictedEvent) {
		if !event.entries().is_empty() {
			let _ = self.actor_ref.send(MetricMessage::CdcEvicted(event.clone()));
		}
	}
}

#[derive(Clone)]
pub struct ProfilerSnapshotListener {
	actor_ref: ActorRef<MetricMessage>,
}

impl ProfilerSnapshotListener {
	pub fn new(actor_ref: ActorRef<MetricMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<ProfilerSnapshotEvent> for ProfilerSnapshotListener {
	fn on(&self, event: &ProfilerSnapshotEvent) {
		if !event.rows().is_empty() {
			let _ = self.actor_ref.send(MetricMessage::ProfilerSnapshot(event.clone()));
		}
	}
}
