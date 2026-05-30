// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_profiler::{category::CategorySet, intern::DimInterner, layer::ProfilerLayer, sink::ProfilerSink};
use reifydb_runtime::{context::clock::Clock, shutdown::Shutdown, sync::rwlock::RwLock};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::{info, instrument};

use crate::{accumulator::ProfilerAccumulator, histograms, reader::ProfilerReader};

pub struct ProfilerSubsystem {
	running: AtomicBool,
	categories: CategorySet,
	interner: Arc<DimInterner>,
	accumulator: Arc<RwLock<ProfilerAccumulator>>,
	sink: Arc<dyn ProfilerSink>,
	clock: Clock,
}

impl ProfilerSubsystem {
	pub fn new(
		enabled: bool,
		categories: CategorySet,
		interner: Arc<DimInterner>,
		accumulator: Arc<RwLock<ProfilerAccumulator>>,
		sink: Arc<dyn ProfilerSink>,
		clock: Clock,
	) -> Self {
		if enabled {
			histograms::register_all();
		}
		info!("Profiler subsystem started (enabled={}, categories={:?})", enabled, categories);
		Self {
			running: AtomicBool::new(true),
			categories,
			interner,
			accumulator,
			sink,
			clock,
		}
	}

	pub fn layer(&self) -> ProfilerLayer {
		ProfilerLayer::new(
			Arc::clone(&self.sink),
			self.categories,
			Arc::clone(&self.interner),
			self.clock.clone(),
		)
	}

	pub fn reader(&self) -> ProfilerReader {
		ProfilerReader::new(Arc::clone(&self.accumulator))
	}

	pub fn categories(&self) -> CategorySet {
		self.categories
	}

	pub fn interner(&self) -> Arc<DimInterner> {
		Arc::clone(&self.interner)
	}

	pub fn accumulator(&self) -> Arc<RwLock<ProfilerAccumulator>> {
		Arc::clone(&self.accumulator)
	}
}

impl Shutdown for ProfilerSubsystem {
	#[instrument(name = "profiler::subsystem::shutdown", level = "debug", skip(self))]
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}
		info!("Profiler subsystem shutting down");
	}
}

impl Subsystem for ProfilerSubsystem {
	fn name(&self) -> &'static str {
		"sub-profiler"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

impl HasVersion for ProfilerSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Always-on profiling subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
