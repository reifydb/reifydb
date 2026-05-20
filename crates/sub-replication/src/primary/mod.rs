// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod service;

use std::sync::Arc;

use reifydb_core::event::{EventListener, metric::CdcWrittenEvent};
use tokio::sync::Notify;

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

impl EventListener<CdcWrittenEvent> for CdcNotifyListener {
	fn on(&self, _event: &CdcWrittenEvent) {
		self.notify.notify_waiters();
	}
}
