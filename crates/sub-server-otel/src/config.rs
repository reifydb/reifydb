// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

/// OpenTelemetry exporter backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExporterType {
	/// OTLP exporter (gRPC) - works with Jaeger, OpenTelemetry Collector, and other backends
	Otlp,
}

/// Configurator for the OpenTelemetry server subsystem.
pub struct OtelConfigurator {
	service_name: String,
	service_version: String,
	exporter_type: ExporterType,
	endpoint: String,
	sample_ratio: f64,
	max_export_batch_size: usize,
	scheduled_delay: Duration,
	max_queue_size: usize,
	export_timeout: Duration,
}

impl Default for OtelConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl OtelConfigurator {
	/// Create a new OtelConfigurator with default values.
	pub fn new() -> Self {
		Self {
			service_name: "reifydb".to_string(),
			service_version: env!("CARGO_PKG_VERSION").to_string(),
			exporter_type: ExporterType::Otlp,
			endpoint: "http://localhost:4317".to_string(),
			sample_ratio: 1.0,
			max_export_batch_size: 512,
			scheduled_delay: Duration::from_millis(5000),
			max_queue_size: 2048,
			export_timeout: Duration::from_secs(30),
		}
	}

	/// Set the service name.
	pub fn service_name(mut self, name: impl Into<String>) -> Self {
		self.service_name = name.into();
		self
	}

	/// Set the service version.
	pub fn service_version(mut self, version: impl Into<String>) -> Self {
		self.service_version = version.into();
		self
	}

	/// Set the exporter type.
	pub fn exporter_type(mut self, exporter_type: ExporterType) -> Self {
		self.exporter_type = exporter_type;
		self
	}

	/// Set the endpoint (OTLP or Jaeger agent).
	pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.endpoint = endpoint.into();
		self
	}

	/// Set the sampling ratio (0.0 to 1.0).
	pub fn sample_ratio(mut self, ratio: f64) -> Self {
		self.sample_ratio = ratio.clamp(0.0, 1.0);
		self
	}

	/// Set the maximum export batch size.
	pub fn max_export_batch_size(mut self, size: usize) -> Self {
		self.max_export_batch_size = size;
		self
	}

	/// Set the scheduled delay for batch export.
	pub fn scheduled_delay(mut self, delay: Duration) -> Self {
		self.scheduled_delay = delay;
		self
	}

	/// Set the maximum queue size.
	pub fn max_queue_size(mut self, size: usize) -> Self {
		self.max_queue_size = size;
		self
	}

	/// Set the export timeout.
	pub fn export_timeout(mut self, timeout: Duration) -> Self {
		self.export_timeout = timeout;
		self
	}

	pub fn configure(self) -> OtelConfig {
		OtelConfig {
			service_name: self.service_name,
			service_version: self.service_version,
			exporter_type: self.exporter_type,
			endpoint: self.endpoint,
			sample_ratio: self.sample_ratio,
			max_export_batch_size: self.max_export_batch_size,
			scheduled_delay: self.scheduled_delay,
			max_queue_size: self.max_queue_size,
			export_timeout: self.export_timeout,
		}
	}
}

/// Immutable configuration for the OpenTelemetry server subsystem.
#[derive(Clone, Debug)]
pub struct OtelConfig {
	/// Service name for traces (appears in Jaeger UI)
	pub service_name: String,
	/// Service version
	pub service_version: String,
	/// Exporter type
	pub exporter_type: ExporterType,
	/// OTLP endpoint (e.g., "http://localhost:4317" for gRPC)
	pub endpoint: String,
	/// Sampling ratio (0.0 to 1.0)
	pub sample_ratio: f64,
	/// Maximum batch export size
	pub max_export_batch_size: usize,
	/// Scheduled delay for batch export
	pub scheduled_delay: Duration,
	/// Maximum queue size for traces
	pub max_queue_size: usize,
	/// Export timeout
	pub export_timeout: Duration,
}

impl Default for OtelConfig {
	fn default() -> Self {
		OtelConfigurator::new().configure()
	}
}
