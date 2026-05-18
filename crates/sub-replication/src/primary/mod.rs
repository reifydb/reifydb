// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
