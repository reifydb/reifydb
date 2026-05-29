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
use reifydb_runtime::{context::clock::Clock, sync::rwlock::RwLock};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::Result;
use tracing::{info, instrument};

use crate::{accumulator::ProfilerAccumulator, histograms, reader::ProfilerReader};

pub struct ProfilerSubsystem {
	running: AtomicBool,
	enabled: bool,
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
		Self {
			running: AtomicBool::new(false),
			enabled,
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

impl Subsystem for ProfilerSubsystem {
	fn name(&self) -> &'static str {
		"sub-profiler"
	}

	#[instrument(name = "profiler::subsystem::start", level = "debug", skip(self))]
	fn start(&mut self) -> Result<()> {
		if self.enabled {
			histograms::register_all();
		}
		self.running.store(true, Ordering::Release);
		info!("Profiler subsystem started (enabled={}, categories={:?})", self.enabled, self.categories);
		Ok(())
	}

	#[instrument(name = "profiler::subsystem::shutdown", level = "debug", skip(self))]
	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}
		info!("Profiler subsystem shutting down");
		Ok(())
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
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
