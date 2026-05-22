// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};

use reifydb_core::actors::cdc::CdcPollMessage;
use reifydb_runtime::{actor::mailbox::ActorRef, sync::mutex::Mutex};

struct WakeHandle {
	armed: Arc<AtomicBool>,
	actor_ref: ActorRef<CdcPollMessage>,
}

#[derive(Clone)]
pub struct CdcWakeRegistry {
	handles: Arc<Mutex<Vec<WakeHandle>>>,
}

impl std::fmt::Debug for CdcWakeRegistry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CdcWakeRegistry").finish_non_exhaustive()
	}
}

impl Default for CdcWakeRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl CdcWakeRegistry {
	pub fn new() -> Self {
		Self {
			handles: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register(&self, armed: Arc<AtomicBool>, actor_ref: ActorRef<CdcPollMessage>) {
		self.handles.lock().push(WakeHandle {
			armed,
			actor_ref,
		});
	}

	pub fn notify_all(&self) {
		let handles = self.handles.lock();
		for handle in handles.iter() {
			if handle.armed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
				let _ = handle.actor_ref.send(CdcPollMessage::Poll);
			}
		}
	}
}
