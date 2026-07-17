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
use reifydb_runtime::shutdown::Shutdown;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::info;

use crate::domains::runtime::SampleReader;

pub struct MetricSubsystem {
	running: Arc<AtomicBool>,
	sample_reader: SampleReader,
}

impl MetricSubsystem {
	pub fn new(sample_reader: SampleReader) -> Self {
		info!("Metric subsystem started");
		Self {
			running: Arc::new(AtomicBool::new(true)),
			sample_reader,
		}
	}

	pub fn sample_reader(&self) -> SampleReader {
		self.sample_reader.clone()
	}
}

impl HasVersion for MetricSubsystem {
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

impl Shutdown for MetricSubsystem {
	fn shutdown(&self) {
		self.running.store(false, Ordering::SeqCst);
	}
}

impl Subsystem for MetricSubsystem {
	fn name(&self) -> &'static str {
		"Metric"
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
