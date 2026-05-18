// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	encoded::key::EncodedKey,
	event::{EventListener, metric::MultiCommittedEvent},
	interface::store::EntryKind,
};
use reifydb_runtime::actor::mailbox::ActorRef;

use super::actor::FlushMessage;
use crate::store::router::classify_key;

pub struct FlushEventListener {
	actor_ref: ActorRef<FlushMessage>,
}

impl FlushEventListener {
	pub fn new(actor_ref: ActorRef<FlushMessage>) -> Self {
		Self {
			actor_ref,
		}
	}
}

impl EventListener<MultiCommittedEvent> for FlushEventListener {
	fn on(&self, event: &MultiCommittedEvent) {
		if event.writes().is_empty() && event.deletes().is_empty() {
			return;
		}

		let mut sets: HashMap<EntryKind, Vec<EncodedKey>> = HashMap::new();
		let mut tombstones: HashMap<EntryKind, Vec<EncodedKey>> = HashMap::new();

		for write in event.writes() {
			let kind = classify_key(&write.key);
			sets.entry(kind).or_default().push(write.key.clone());
		}
		for delete in event.deletes() {
			let kind = classify_key(&delete.key);
			tombstones.entry(kind).or_default().push(delete.key.clone());
		}

		let _ = self.actor_ref.send(FlushMessage::Dirty {
			version: *event.version(),
			sets,
			tombstones,
		});
	}
}
