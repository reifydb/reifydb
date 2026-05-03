// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	error,
	result::Result as StdResult,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
};

use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
	Resource,
	trace::{
		BatchConfigBuilder, BatchSpanProcessor, RandomIdGenerator, Sampler, SdkTracerProvider,
		Tracer as SdkTracer,
	},
};
use reifydb_core::{
	error::CoreError,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::{debug, error, info};

use crate::config::OtelConfig;

pub struct OtelSubsystem {
	config: OtelConfig,

	running: Arc<AtomicBool>,

	tracer_provider: Arc<Mutex<Option<SdkTracerProvider>>>,

	runtime: SharedRuntime,
}

impl OtelSubsystem {
	pub fn new(config: OtelConfig, runtime: SharedRuntime) -> Self {
		Self {
			config,
			running: Arc::new(AtomicBool::new(false)),
			tracer_provider: Arc::new(Mutex::new(None)),
			runtime,
		}
	}

	pub fn config(&self) -> &OtelConfig {
		&self.config
	}

	pub fn tracer(&self) -> Option<SdkTracer> {
		self.tracer_provider
			.try_lock()
			.ok()
			.and_then(|guard| guard.as_ref().map(|provider| provider.tracer("reifydb")))
	}

	fn build_provider_in_runtime(&self) -> Result<SdkTracerProvider> {
		#[cfg(not(feature = "otlp"))]
		{
			return Err(CoreError::SubsystemFeatureDisabled {
				feature: "otlp".to_string(),
			}
			.into());
		}
		#[cfg(feature = "otlp")]
		{
			let _guard = self.runtime.handle().enter();
			self.build_otlp_tracer_provider().map_err(|e| {
				CoreError::SubsystemInitFailed {
					subsystem: "OpenTelemetry".to_string(),
					reason: e.to_string(),
				}
				.into()
			})
		}
	}

	#[inline]
	fn install_provider(&self, provider: SdkTracerProvider) {
		global::set_tracer_provider(provider.clone());
		*self.tracer_provider.lock().unwrap() = Some(provider);
	}

	#[cfg(feature = "otlp")]
	fn build_otlp_tracer_provider(&self) -> StdResult<SdkTracerProvider, Box<dyn error::Error>> {
		let resource = Resource::builder()
			.with_service_name(self.config.service_name.clone())
			.with_attributes([KeyValue::new("service.version", self.config.service_version.clone())])
			.build();

		let exporter = SpanExporter::builder()
			.with_tonic()
			.with_endpoint(&self.config.endpoint)
			.with_timeout(self.config.export_timeout)
			.build()?;

		let batch_config = BatchConfigBuilder::default()
			.with_max_export_batch_size(self.config.max_export_batch_size)
			.with_scheduled_delay(self.config.scheduled_delay)
			.with_max_queue_size(self.config.max_queue_size)
			.build();

		let batch_processor = BatchSpanProcessor::builder(exporter).with_batch_config(batch_config).build();

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

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}
		let provider = self.build_provider_in_runtime()?;
		self.install_provider(provider);
		self.running.store(true, Ordering::SeqCst);
		info!(
			service = %self.config.service_name,
			endpoint = %self.config.endpoint,
			exporter = ?self.config.exporter_type,
			"OpenTelemetry subsystem started"
		);
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if self.running.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_err() {
			return Ok(());
		}

		if let Some(provider) = self.tracer_provider.lock().unwrap().take() {
			if let Err(e) = provider.shutdown() {
				error!("Error shutting down tracer provider: {:?}", e);
			} else {
				debug!("Tracer provider shutdown complete");
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
