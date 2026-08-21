// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::value::duration::Duration;

use crate::{
	adaptive::{AdaptiveKeyFilter, RebuildHandle},
	config::FilterConfig,
	source::KeyFilterSource,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverProgress {
	Idle,
	Started,
	Scanning,
	Committed,
}

pub struct RebuildDriver {
	filter: Arc<AdaptiveKeyFilter>,
	source: Box<dyn KeyFilterSource>,
	config: FilterConfig,
	rebuild: Option<RebuildHandle>,
}

impl RebuildDriver {
	pub fn new(filter: Arc<AdaptiveKeyFilter>, source: Box<dyn KeyFilterSource>, config: FilterConfig) -> Self {
		Self {
			filter,
			source,
			config,
			rebuild: None,
		}
	}

	pub fn name(&self) -> &'static str {
		self.source.name()
	}

	pub fn interval(&self) -> Duration {
		self.config.interval
	}

	pub fn step(&mut self) -> DriverProgress {
		match self.rebuild.take() {
			Some(handle) => self.scan(handle),
			None => self.evaluate(),
		}
	}

	fn evaluate(&mut self) -> DriverProgress {
		let source_len = self.source.estimated_len();
		if !self.filter.is_enabled() {
			return self.begin(source_len);
		}

		let metrics = self.filter.metrics();
		if metrics.fill_ratio > self.config.fill_trigger {
			return self.begin(source_len);
		}
		if source_len > 0 && (metrics.estimated_keys as f64) > (source_len as f64) * self.config.drift_trigger {
			return self.begin(source_len);
		}
		DriverProgress::Idle
	}

	fn begin(&mut self, source_len: u64) -> DriverProgress {
		let size = (((source_len as f64) * self.config.size_headroom) as u64).max(self.config.min_size_keys);
		self.source.restart();
		self.rebuild = Some(self.filter.begin_rebuild(size));
		DriverProgress::Started
	}

	fn scan(&mut self, handle: RebuildHandle) -> DriverProgress {
		let slice = self.source.next_slice(self.config.scan_budget);
		handle.feed(&slice.hashes);
		if slice.exhausted {
			self.filter.commit_rebuild(handle);
			return DriverProgress::Committed;
		}
		self.rebuild = Some(handle);
		DriverProgress::Scanning
	}
}
