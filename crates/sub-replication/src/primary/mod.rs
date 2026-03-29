// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod service;

use std::sync::Arc;

use reifydb_core::event::{EventListener, metric::CdcStatsRecordedEvent};
use tokio::sync::Notify;

/// Bridges the EventBus (actor-based) to tokio::sync::Notify so that
/// replication streaming tasks wake immediately when new CDC entries are written.
pub struct CdcNotifyListener {
	notify: Arc<Notify>,
}

impl CdcNotifyListener {
	pub fn new(notify: Arc<Notify>) -> Self {
		Self {
			notify,
		}
	}
}

impl EventListener<CdcStatsRecordedEvent> for CdcNotifyListener {
	fn on(&self, _event: &CdcStatsRecordedEvent) {
		self.notify.notify_waiters();
	}
}
