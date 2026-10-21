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
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSpawner},
	},
	context::clock::Clock,
	shutdown::{Shutdown, ShutdownErrors},
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::Result;
use tracing::{info, instrument};

use super::{
	accumulator::ProfilerAccumulator,
	actor::{ProfilerCollectorActor, ProfilerMessage},
	instruments::ProfilerInstruments,
	reader::ProfilerReader,
};

pub struct ProfilerCollector {
	scope: ActorSpawner,
	actor_ref: ActorRef<ProfilerMessage>,
	handle: Mutex<Option<ActorHandle<ProfilerMessage>>>,
}

impl ProfilerCollector {
	pub fn spawn(spawner: &ActorSpawner, actor: ProfilerCollectorActor) -> Self {
		let scope = spawner.scope();
		let handle = scope.spawn_coordination("profile-collector", actor);
		Self {
			scope,
			actor_ref: handle.actor_ref().clone(),
			handle: Mutex::new(Some(handle)),
		}
	}

	pub fn actor_ref(&self) -> &ActorRef<ProfilerMessage> {
		&self.actor_ref
	}

	fn stop(&self) -> Result<()> {
		let mut errors = ShutdownErrors::new("profiler");
		self.scope.shutdown();
		if let Some(handle) = self.handle.lock().take()
			&& let Err(err) = handle.join()
		{
			errors.fail("collector", err);
		}
		errors.finish()
	}
}

pub struct ProfilerSubsystem {
	running: AtomicBool,
	categories: CategorySet,
	interner: Arc<DimInterner>,
	accumulator: Arc<RwLock<ProfilerAccumulator>>,
	instruments: Arc<ProfilerInstruments>,
	sink: Arc<dyn ProfilerSink>,
	clock: Clock,
	collector: Option<ProfilerCollector>,
}

impl ProfilerSubsystem {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		categories: CategorySet,
		interner: Arc<DimInterner>,
		accumulator: Arc<RwLock<ProfilerAccumulator>>,
		instruments: Arc<ProfilerInstruments>,
		sink: Arc<dyn ProfilerSink>,
		clock: Clock,
		collector: Option<ProfilerCollector>,
	) -> Self {
		info!("Profiler subsystem started (categories={:?})", categories);
		Self {
			running: AtomicBool::new(true),
			categories,
			interner,
			accumulator,
			instruments,
			sink,
			clock,
			collector,
		}
	}

	pub fn collector(&self) -> Option<&ActorRef<ProfilerMessage>> {
		self.collector.as_ref().map(ProfilerCollector::actor_ref)
	}

	pub fn instruments(&self) -> &Arc<ProfilerInstruments> {
		&self.instruments
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
}

impl Shutdown for ProfilerSubsystem {
	#[instrument(name = "profiler::subsystem::shutdown", level = "info", skip(self))]
	fn shutdown(&self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
			return Ok(());
		}
		info!("Profiler subsystem shutting down");
		match &self.collector {
			Some(collector) => collector.stop(),
			None => Ok(()),
		}
	}
}

impl Subsystem for ProfilerSubsystem {
	fn name(&self) -> &'static str {
		"profiler"
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
			name: "sub-profiler".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Always-on profiling subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
