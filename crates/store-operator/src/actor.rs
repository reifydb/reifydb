// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::{actor::mailbox::ActorRef, sync::mutex::Mutex};

#[derive(Default)]
pub enum Waker<M> {
	#[default]
	Silent,
	Spawned(ActorRef<M>),
	Recording(Arc<Mutex<Vec<M>>>),
}

impl<M> Clone for Waker<M> {
	fn clone(&self) -> Self {
		match self {
			Self::Silent => Self::Silent,
			Self::Spawned(actor) => Self::Spawned(actor.clone()),
			Self::Recording(seen) => Self::Recording(Arc::clone(seen)),
		}
	}
}

impl<M> Waker<M> {
	pub fn recording() -> (Self, Arc<Mutex<Vec<M>>>) {
		let seen = Arc::new(Mutex::new(Vec::new()));
		(Self::Recording(Arc::clone(&seen)), seen)
	}
}

impl<M: Send> Waker<M> {
	pub fn wake(&self, message: M) {
		match self {
			Self::Silent => {}
			Self::Spawned(actor) => {
				let _ = actor.send(message);
			}
			Self::Recording(seen) => seen.lock().push(message),
		}
	}
}
