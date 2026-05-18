// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::EventListener;
use reifydb_profiler::event::{ProfilerScopeBatchEvent, ProfilerScopeClosedEvent};
use reifydb_runtime::actor::mailbox::ActorRef;

use crate::actor::ProfilerMessage;

#[derive(Clone)]
pub struct ProfilerScopeClosedListener {
	actor_ref: ActorRef<ProfilerMessage>,
}

impl ProfilerScopeClosedListener {
	pub fn new(actor_ref: ActorRef<ProfilerMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<ProfilerScopeClosedEvent> for ProfilerScopeClosedListener {
	fn on(&self, event: &ProfilerScopeClosedEvent) {
		let _ = self.actor_ref.send(ProfilerMessage::ScopeClosed(event.summary().clone()));
	}
}

#[derive(Clone)]
pub struct ProfilerScopeBatchListener {
	actor_ref: ActorRef<ProfilerMessage>,
}

impl ProfilerScopeBatchListener {
	pub fn new(actor_ref: ActorRef<ProfilerMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<ProfilerScopeBatchEvent> for ProfilerScopeBatchListener {
	fn on(&self, event: &ProfilerScopeBatchEvent) {
		let _ = self.actor_ref.send(ProfilerMessage::ScopeBatch(event.summary().clone()));
	}
}
