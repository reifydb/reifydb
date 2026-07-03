// SPDX-License-Identifier: Apache-2.0
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
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::clock::Clock,
	shutdown::Shutdown,
	sync::{mutex::Mutex, rwlock::RwLock},
};
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
	snapshot_scope: Mutex<Option<ActorSpawner>>,
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
			snapshot_scope: Mutex::new(None),
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

	pub(crate) fn set_snapshot_scope(&self, scope: ActorSpawner) {
		*self.snapshot_scope.lock() = Some(scope);
	}

	pub fn snapshot_persistence_enabled(&self) -> bool {
		self.snapshot_scope.lock().is_some()
	}
}

impl Shutdown for ProfilerSubsystem {
	#[instrument(name = "profiler::subsystem::shutdown", level = "info", skip(self))]
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return;
		}
		info!("Profiler subsystem shutting down");
		drop(self.snapshot_scope.lock().take());
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
