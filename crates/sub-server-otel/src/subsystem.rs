// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! OpenTelemetry server subsystem implementing the ReifyDB Subsystem trait.

use std::{
	any::Any,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
};

use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer as SdkTracer};
use reifydb_core::{
	error::diagnostic::subsystem::init_failed,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::error;

use crate::config::OtelConfig;

/// OpenTelemetry subsystem.
///
/// Manages OpenTelemetry tracing integration with support for:
/// - OTLP and Jaeger exporters
/// - Graceful startup and shutdown with proper trace flushing
/// - Integration with existing tracing infrastructure
/// - Health monitoring
///
/// # Architecture Note
///
/// This subsystem creates and manages an OpenTelemetry tracer provider.
/// To integrate with the tracing ecosystem, you must also configure
/// `sub-tracing` to include the OpenTelemetry layer using the
/// `with_layer()` method (see sub-tracing documentation).
///
/// # Example
///
/// ```ignore
/// use reifydb_sub_server_otel::{OtelConfig, OtelSubsystem, ExporterType};
///
/// let config = OtelConfig::new()
///     .service_name("my-service")
///     .endpoint("http://localhost:4317");
///
/// let mut otel = OtelSubsystem::new(config);
/// otel.start()?;
/// // Tracer provider is now set globally and traces are being exported
///
/// otel.shutdown()?;
/// // All traces flushed and exported
/// ```
pub struct OtelSubsystem {
	/// Configuration
	config: OtelConfig,
	/// Flag indicating if the subsystem is running
	running: Arc<AtomicBool>,
	/// The tracer provider (held to prevent premature drop)
	tracer_provider: Arc<Mutex<Option<SdkTracerProvider>>>,
	/// Shared runtime for async operations
	runtime: SharedRuntime,
}

impl OtelSubsystem {
	/// Create a new OpenTelemetry subsystem.
	///
	/// # Arguments
	///
	/// * `config` - OpenTelemetry configuration
	/// * `runtime` - Shared runtime
	pub fn new(config: OtelConfig, runtime: SharedRuntime) -> Self {
		Self {
			config,
			running: Arc::new(AtomicBool::new(false)),
			tracer_provider: Arc::new(Mutex::new(None)),
			runtime,
		}
	}

	/// Get the configuration
	pub fn config(&self) -> &OtelConfig {
		&self.config
	}

	/// Get a tracer from the initialized provider.
	///
	/// Returns None if the subsystem hasn't been started yet or if the lock is contended.
	/// Uses try_lock() to avoid blocking in sync context with async mutex.
	pub fn tracer(&self) -> Option<SdkTracer> {
		self.tracer_provider
			.try_lock()
			.ok()
			.and_then(|guard| guard.as_ref().map(|provider| provider.tracer("reifydb")))
	}

	/// Build the OTLP tracer provider
	#[cfg(feature = "otlp")]
	fn build_otlp_tracer_provider(&self) -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
		use opentelemetry::KeyValue;
		use opentelemetry_otlp::WithExportConfig;
		use opentelemetry_sdk::{
			Resource,
			trace::{BatchConfigBuilder, BatchSpanProcessor, RandomIdGenerator, Sampler},
		};

		// Build resource with service name and version
		let resource = Resource::builder()
			.with_service_name(self.config.service_name.clone())
			.with_attributes([KeyValue::new("service.version", self.config.service_version.clone())])
			.build();

		// Build the OTLP exporter
		let exporter = SpanExporter::builder()
			.with_tonic()
			.with_endpoint(&self.config.endpoint)
			.with_timeout(self.config.export_timeout)
			.build()?;

		// Configure batch processor with our settings
		let batch_config = BatchConfigBuilder::default()
			.with_max_export_batch_size(self.config.max_export_batch_size)
			.with_scheduled_delay(self.config.scheduled_delay)
			.with_max_queue_size(self.config.max_queue_size)
			.build();

		let batch_processor = BatchSpanProcessor::builder(exporter).with_batch_config(batch_config).build();

		// Build the tracer provider
		let provider = SdkTracerProvider::builder()
			.with_span_processor(batch_processor)
			.with_resource(resource)
			.with_sampler(Sampler::TraceIdRatioBased(self.config.sample_ratio))
			.with_id_generator(RandomIdGenerator::default())
			.build();

		Ok(provider)
	}
}

impl HasVersion for OtelSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "OpenTelemetry/Jaeger tracing subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for OtelSubsystem {
	fn name(&self) -> &'static str {
		"OpenTelemetry"
	}

	fn start(&mut self) -> reifydb_type::Result<()> {
		// Idempotent: if already running, return success
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Build the tracer provider (needs runtime context for tonic/hyper)
		#[cfg(not(feature = "otlp"))]
		{
			return Err(error!(reifydb_core::error::diagnostic::subsystem::feature_disabled("otlp")));
		}

		#[cfg(feature = "otlp")]
		let provider = {
			// Enter runtime context for tonic/hyper initialization
			let _guard = self.runtime.handle().enter();
			self.build_otlp_tracer_provider().map_err(|e| error!(init_failed("OpenTelemetry", e)))?
		};

		// Set the global tracer provider
		// This allows tracing-opentelemetry layer to find and use it
		global::set_tracer_provider(provider.clone());

		// Store the provider to prevent premature drop
		*self.tracer_provider.lock().unwrap() = Some(provider);

		self.running.store(true, Ordering::SeqCst);
		tracing::info!(
			service = %self.config.service_name,
			endpoint = %self.config.endpoint,
			exporter = ?self.config.exporter_type,
			"OpenTelemetry subsystem started"
		);

		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_type::Result<()> {
		if !self.running.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
			return Ok(()); // Already shutdown
		}

		if let Some(provider) = self.tracer_provider.lock().unwrap().take() {
			// This ensures all pending traces are exported
			if let Err(e) = provider.shutdown() {
				tracing::error!("Error shutting down tracer provider: {:?}", e);
			} else {
				tracing::debug!("Tracer provider shutdown complete");
			}
		}

		Ok(())
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
