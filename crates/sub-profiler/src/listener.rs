// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::EventListener;
use reifydb_profiler::event::{ProfileScopeBatchEvent, ProfileScopeClosedEvent};
use reifydb_runtime::actor::mailbox::ActorRef;

use crate::actor::ProfilerMessage;

#[derive(Clone)]
pub struct ProfileScopeClosedListener {
	actor_ref: ActorRef<ProfilerMessage>,
}

impl ProfileScopeClosedListener {
	pub fn new(actor_ref: ActorRef<ProfilerMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<ProfileScopeClosedEvent> for ProfileScopeClosedListener {
	fn on(&self, event: &ProfileScopeClosedEvent) {
		let _ = self.actor_ref.send(ProfilerMessage::ScopeClosed(event.summary().clone()));
	}
}

#[derive(Clone)]
pub struct ProfileScopeBatchListener {
	actor_ref: ActorRef<ProfilerMessage>,
}

impl ProfileScopeBatchListener {
	pub fn new(actor_ref: ActorRef<ProfilerMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<ProfileScopeBatchEvent> for ProfileScopeBatchListener {
	fn on(&self, event: &ProfileScopeBatchEvent) {
		let _ = self.actor_ref.send(ProfilerMessage::ScopeBatch(event.summary().clone()));
	}
}
