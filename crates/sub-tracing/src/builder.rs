// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder pattern for configuring the tracing subsystem

use tracing_subscriber::{
	EnvFilter, Layer, Registry,
	fmt::{self, format::FmtSpan},
	layer::SubscriberExt,
	util::SubscriberInitExt,
};

use crate::{backend::ConsoleBuilder, subsystem::TracingSubsystem};

/// Builder for configuring the tracing subsystem with tracing_subscriber
pub struct TracingBuilder {
	filter: Option<String>,
	console_config: Option<ConsoleBuilder>,
	with_spans: bool,
	external_layer: Option<Box<dyn Layer<Registry> + Send + Sync>>,
}

impl Default for TracingBuilder {
	fn default() -> Self {
		Self::new().with_console(|c| c)
	}
}

impl TracingBuilder {
	/// Create a new TracingBuilder with default settings
	pub fn new() -> Self {
		Self {
			filter: None,
			console_config: None,
			with_spans: false,
			external_layer: None,
		}
	}

	/// Configure console output
	///
	/// # Example
	/// ```ignore
	/// TracingBuilder::new()
	///     .with_console(|console| console.color(true).stderr_for_errors(true))
	/// ```
	pub fn with_console<F>(mut self, builder_fn: F) -> Self
	where
		F: FnOnce(ConsoleBuilder) -> ConsoleBuilder,
	{
		let builder = builder_fn(ConsoleBuilder::new());
		self.console_config = Some(builder);
		self
	}

	/// Disable console logging entirely
	///
	/// This is useful when you only want OpenTelemetry tracing without
	/// the performance overhead of console output.
	///
	/// # Example
	/// ```ignore
	/// TracingBuilder::new()
	///     .without_console()  // Disable console output
	///     .with_layer(otel_layer)  // Only use OpenTelemetry
	///     .with_filter("trace")  // Can still filter what spans are recorded
	///     .build()
	/// ```
	pub fn without_console(mut self) -> Self {
		self.console_config = None;
		self
	}

	/// Set the log filter using tracing_subscriber's EnvFilter syntax
	///
	/// # Examples
	/// ```ignore
	/// // Global info level
	/// builder.with_filter("info")
	///
	/// // Per-crate filtering
	/// builder.with_filter("warn,reifydb_engine=debug,reifydb_catalog=trace")
	///
	/// // Filter specific modules
	/// builder.with_filter("reifydb_catalog::transaction=trace")
	///
	/// // Filter by span name
	/// builder.with_filter("reifydb_catalog[slow]=debug")
	/// ```
	pub fn with_filter(mut self, filter: &str) -> Self {
		self.filter = Some(filter.to_string());
		self
	}

	/// Enable span events (enter/exit logging)
	/// This adds more verbose output but helps trace execution flow
	pub fn with_span_events(mut self, enabled: bool) -> Self {
		self.with_spans = enabled;
		self
	}

	/// Add an external layer to the tracing subscriber
	///
	/// This allows other subsystems (like OpenTelemetry) to contribute
	/// a layer to the tracing subscriber before it's initialized.
	///
	/// Note: Only one external layer can be added. If called multiple times,
	/// the last layer will be used.
	///
	/// # Example
	/// ```ignore
	/// let tracer = opentelemetry::global::tracer("reifydb");
	/// let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
	///
	/// TracingBuilder::new()
	///     .with_layer(otel_layer)
	///     .with_filter("info")
	///     .build()
	/// ```
	pub fn with_layer<L>(mut self, layer: L) -> Self
	where
		L: Layer<Registry> + Send + Sync + 'static,
	{
		self.external_layer = Some(Box::new(layer));
		self
	}

	/// Build and initialize the tracing subsystem
	///
	/// This sets up the global tracing subscriber. It should only be called once.
	pub fn build(self) -> TracingSubsystem {
		// Build the filter
		let filter = self
			.filter
			.map(|f| EnvFilter::try_new(&f).unwrap_or_else(|_| EnvFilter::new("info")))
			.unwrap_or_else(|| {
				// Try to get from RUST_LOG env var, fallback to info
				EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
			});

		// Build subscriber with external layer and filter
		let subscriber = tracing_subscriber::registry().with(self.external_layer).with(filter);

		// Conditionally create console layer
		let fmt_layer = if let Some(console_config) = self.console_config {
			let span_events = if self.with_spans {
				FmtSpan::NEW | FmtSpan::CLOSE
			} else {
				FmtSpan::NONE
			};

			Some(fmt::layer()
				.with_ansi(console_config.use_color())
				.with_target(true)
				.with_thread_ids(false)
				.with_thread_names(true)
				.with_file(true)
				.with_line_number(true)
				.with_span_events(span_events))
		} else {
			None
		};

		// Add the console layer (or None if disabled)
		let subscriber = subscriber.with(fmt_layer);

		// Initialize the global subscriber
		// Note: This will fail silently if a subscriber is already set
		let _ = subscriber.try_init();

		TracingSubsystem::new()
	}
}
