// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	actors::metrics::MetricsMessage,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::{
	actor::system::{ActorHandle, ActorSpawner},
	shutdown::{Shutdown, ShutdownErrors},
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::Result;
use tracing::info;

use crate::{
	domains::{runtime::SampleReader, store::StoreReader},
	sampler::SamplerMessage,
};

pub struct MetricsSubsystem {
	running: Arc<AtomicBool>,
	sample_reader: SampleReader,
	store_reader: StoreReader,
	scope: ActorSpawner,
	sampler: Mutex<Option<ActorHandle<SamplerMessage>>>,
	flush: Mutex<Option<ActorHandle<MetricsMessage>>>,
}

impl MetricsSubsystem {
	pub fn new(
		sample_reader: SampleReader,
		store_reader: StoreReader,
		scope: ActorSpawner,
		sampler: ActorHandle<SamplerMessage>,
		flush: ActorHandle<MetricsMessage>,
	) -> Self {
		info!("Metric subsystem started");
		Self {
			running: Arc::new(AtomicBool::new(true)),
			sample_reader,
			store_reader,
			scope,
			sampler: Mutex::new(Some(sampler)),
			flush: Mutex::new(Some(flush)),
		}
	}

	pub fn sample_reader(&self) -> SampleReader {
		self.sample_reader.clone()
	}

	pub fn store_reader(&self) -> StoreReader {
		self.store_reader.clone()
	}
}

impl HasVersion for MetricsSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Metrics collection and persistence subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for MetricsSubsystem {
	fn shutdown(&self) -> Result<()> {
		if !self.running.swap(false, Ordering::SeqCst) {
			return Ok(());
		}
		let mut errors = ShutdownErrors::new("metrics");
		self.scope.shutdown();
		if let Some(handle) = self.sampler.lock().take()
			&& let Err(err) = handle.join()
		{
			errors.fail("sampler", err);
		}
		if let Some(handle) = self.flush.lock().take()
			&& let Err(err) = handle.join()
		{
			errors.fail("flush", err);
		}
		errors.finish()
	}
}

impl Subsystem for MetricsSubsystem {
	fn name(&self) -> &'static str {
		"metrics"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Not running".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
