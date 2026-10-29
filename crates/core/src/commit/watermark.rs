// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_value::{Result, error::Error};

use crate::{common::CommitVersion, error::diagnostic::flow::flow_error, interface::catalog::flow::FlowId};

#[derive(Debug, Clone, Default)]
pub struct CdcConsumerWatermark {
	progress: Arc<AtomicU64>,
}

impl CdcConsumerWatermark {
	pub fn new() -> Self {
		Self {
			progress: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.progress.load(Ordering::Acquire))
	}

	pub fn store(&self, v: CommitVersion) {
		self.progress.store(v.0, Ordering::Release);
	}
}

#[derive(Clone)]
pub struct FlowCaughtUpWatermark {
	sample: Arc<dyn Fn() -> CommitVersion + Send + Sync>,
	poisoned: Arc<dyn Fn() -> Vec<(FlowId, String)> + Send + Sync>,
	stalled: Arc<dyn Fn() -> Vec<FlowId> + Send + Sync>,
}

impl FlowCaughtUpWatermark {
	pub fn new<F, P, S>(sample: F, poisoned: P, stalled: S) -> Self
	where
		F: Fn() -> CommitVersion + Send + Sync + 'static,
		P: Fn() -> Vec<(FlowId, String)> + Send + Sync + 'static,
		S: Fn() -> Vec<FlowId> + Send + Sync + 'static,
	{
		Self {
			sample: Arc::new(sample),
			poisoned: Arc::new(poisoned),
			stalled: Arc::new(stalled),
		}
	}

	pub fn get(&self) -> CommitVersion {
		(self.sample)()
	}

	pub fn poisoned(&self) -> Vec<(FlowId, String)> {
		(self.poisoned)()
	}

	pub fn stalled(&self) -> Vec<FlowId> {
		(self.stalled)()
	}

	pub fn ensure_healthy(&self) -> Result<()> {
		let poisoned = self.poisoned();
		if !poisoned.is_empty() {
			let flows: Vec<String> =
				poisoned.iter().map(|(id, reason)| format!("flow {}: {}", id.0, reason)).collect();
			return Err(Error(Box::new(flow_error(format!(
				"{} deferred flow(s) poisoned: {}",
				poisoned.len(),
				flows.join("; ")
			)))));
		}
		let stalled = self.stalled();
		if !stalled.is_empty() {
			let flows: Vec<String> = stalled.iter().map(|id| format!("flow {}", id.0)).collect();
			return Err(Error(Box::new(flow_error(format!(
				"{} deferred flow(s) stalled with input pending: {}",
				stalled.len(),
				flows.join("; ")
			)))));
		}
		Ok(())
	}
}
