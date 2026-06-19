// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use tracing::Subscriber;
use tracing_subscriber::{
	EnvFilter, Layer, Registry,
	filter::LevelFilter,
	fmt::{self, format::FmtSpan},
	layer::SubscriberExt,
	registry,
	registry::LookupSpan,
	util::SubscriberInitExt,
};

use crate::{backend::console_builder::ConsoleBuilder, subsystem::TracingSubsystem};

pub struct TracingConfigurator {
	filter: Option<String>,
	console_config: Option<ConsoleBuilder>,
	with_spans: bool,
	external_layer: Option<Box<dyn Layer<Registry> + Send + Sync>>,
	external_layer_filter: Option<LevelFilter>,
}

impl Default for TracingConfigurator {
	fn default() -> Self {
		Self::new().with_console(|c| c)
	}
}

impl TracingConfigurator {
	pub fn new() -> Self {
		Self {
			filter: None,
			console_config: None,
			with_spans: false,
			external_layer: None,
			external_layer_filter: None,
		}
	}

	pub fn with_console<F>(mut self, builder_fn: F) -> Self
	where
		F: FnOnce(ConsoleBuilder) -> ConsoleBuilder,
	{
		let builder = builder_fn(ConsoleBuilder::new());
		self.console_config = Some(builder);
		self
	}

	pub fn without_console(mut self) -> Self {
		self.console_config = None;
		self
	}

	pub fn with_filter(mut self, filter: &str) -> Self {
		self.filter = Some(filter.to_string());
		self
	}

	pub fn with_span_events(mut self, enabled: bool) -> Self {
		self.with_spans = enabled;
		self
	}

	pub fn with_layer<L>(mut self, layer: L) -> Self
	where
		L: Layer<Registry> + Send + Sync + 'static,
	{
		self.external_layer = Some(Box::new(layer));
		self
	}

	pub fn with_layer_filter(mut self, filter: LevelFilter) -> Self {
		self.external_layer_filter = Some(filter);
		self
	}

	pub fn configure(self) -> TracingSubsystem {
		let env_filter = build_filter(self.filter.as_deref());
		let console_config = self.console_config;
		let with_spans = self.with_spans;
		let _ = match (self.external_layer, self.external_layer_filter) {
			(Some(layer), Some(layer_filter)) => registry()
				.with(layer.with_filter(layer_filter))
				.with(build_console_layer(console_config.as_ref(), with_spans)
					.map(|f| f.with_filter(env_filter)))
				.try_init(),
			(Some(layer), None) => registry()
				.with(layer)
				.with(build_console_layer(console_config.as_ref(), with_spans)
					.map(|f| f.with_filter(env_filter)))
				.try_init(),
			(None, _) => registry()
				.with(build_console_layer(console_config.as_ref(), with_spans)
					.map(|f| f.with_filter(env_filter)))
				.try_init(),
		};
		TracingSubsystem::new()
	}
}

#[inline]
fn build_filter(filter: Option<&str>) -> EnvFilter {
	match filter {
		Some(f) => EnvFilter::try_new(f).unwrap_or_else(|_| EnvFilter::new("info")),
		None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
	}
}

#[inline]
fn build_console_layer<S>(console_config: Option<&ConsoleBuilder>, with_spans: bool) -> Option<fmt::Layer<S>>
where
	S: Subscriber + for<'a> LookupSpan<'a>,
{
	let console_config = console_config?;
	let span_events = if with_spans {
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
}
