// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_sdk::{
	connector::{
		sink::{FFISink, FFISinkMetadata},
		source::{FFISource, FFISourceMetadata},
	},
	error::{FFIError, Result as SdkResult},
};
use reifydb_type::value::Value;

type SourceFactory = Arc<dyn Fn(&HashMap<String, Value>) -> SdkResult<Box<dyn FFISource>> + Send + Sync>;
type SinkFactory = Arc<dyn Fn(&HashMap<String, Value>) -> SdkResult<Box<dyn FFISink>> + Send + Sync>;

pub struct ConnectorRegistry {
	sources: HashMap<String, SourceFactory>,
	sinks: HashMap<String, SinkFactory>,
}

impl ConnectorRegistry {
	pub fn new() -> Self {
		Self {
			sources: HashMap::new(),
			sinks: HashMap::new(),
		}
	}

	pub fn register_source<S: FFISource + FFISourceMetadata>(&mut self) {
		let name = S::NAME.to_string();
		self.sources.insert(
			name,
			Arc::new(|config| {
				let source = S::new(config)?;
				Ok(Box::new(source) as Box<dyn FFISource>)
			}),
		);
	}

	pub fn register_sink<S: FFISink + FFISinkMetadata>(&mut self) {
		let name = S::NAME.to_string();
		self.sinks.insert(
			name,
			Arc::new(|config| {
				let sink = S::new(config)?;
				Ok(Box::new(sink) as Box<dyn FFISink>)
			}),
		);
	}

	pub fn create_source(&self, name: &str, config: &HashMap<String, Value>) -> SdkResult<Box<dyn FFISource>> {
		let factory = self
			.sources
			.get(name)
			.ok_or_else(|| FFIError::Configuration(format!("unknown source connector: {}", name)))?;
		factory(config)
	}

	pub fn create_sink(&self, name: &str, config: &HashMap<String, Value>) -> SdkResult<Box<dyn FFISink>> {
		let factory = self
			.sinks
			.get(name)
			.ok_or_else(|| FFIError::Configuration(format!("unknown sink connector: {}", name)))?;
		factory(config)
	}

	pub fn has_source(&self, name: &str) -> bool {
		self.sources.contains_key(name)
	}

	pub fn has_sink(&self, name: &str) -> bool {
		self.sinks.contains_key(name)
	}
}

impl Default for ConnectorRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_sdk::{
		connector::{
			sink::SinkRecord,
			source::{SourceBatch, SourceEmitter, SourceMode},
		},
		error::Result,
		operator::column::OperatorColumn,
	};

	use super::*;

	struct MockSource;

	impl FFISourceMetadata for MockSource {
		const NAME: &'static str = "mock";
		const VERSION: &'static str = "0.1.0";
		const DESCRIPTION: &'static str = "Mock source for testing";
		const MODE: SourceMode = SourceMode::Pull;
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	}

	impl FFISource for MockSource {
		fn new(_config: &HashMap<String, Value>) -> Result<Self> {
			Ok(MockSource)
		}

		fn poll(&mut self, _checkpoint: Option<&[u8]>) -> Result<SourceBatch> {
			Ok(SourceBatch::empty())
		}

		fn run(&mut self, _checkpoint: Option<&[u8]>, _emitter: SourceEmitter) -> Result<()> {
			unimplemented!("mock source is pull-only")
		}

		fn shutdown(&mut self) -> Result<()> {
			Ok(())
		}
	}

	struct MockSink;

	impl FFISinkMetadata for MockSink {
		const NAME: &'static str = "mock";
		const VERSION: &'static str = "0.1.0";
		const DESCRIPTION: &'static str = "Mock sink for testing";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	}

	impl FFISink for MockSink {
		fn new(_config: &HashMap<String, Value>) -> Result<Self> {
			Ok(MockSink)
		}

		fn write(&mut self, _records: &[SinkRecord]) -> Result<()> {
			Ok(())
		}

		fn shutdown(&mut self) -> Result<()> {
			Ok(())
		}
	}

	#[test]
	fn test_register_and_create_source() {
		let mut registry = ConnectorRegistry::new();
		registry.register_source::<MockSource>();

		assert!(registry.has_source("mock"));
		assert!(!registry.has_source("nonexistent"));

		let source = registry.create_source("mock", &HashMap::new());
		assert!(source.is_ok());
	}

	#[test]
	fn test_register_and_create_sink() {
		let mut registry = ConnectorRegistry::new();
		registry.register_sink::<MockSink>();

		assert!(registry.has_sink("mock"));
		assert!(!registry.has_sink("nonexistent"));

		let sink = registry.create_sink("mock", &HashMap::new());
		assert!(sink.is_ok());
	}

	#[test]
	fn test_unknown_connector_error() {
		let registry = ConnectorRegistry::new();

		let result = registry.create_source("nonexistent", &HashMap::new());
		assert!(result.is_err());

		let result = registry.create_sink("nonexistent", &HashMap::new());
		assert!(result.is_err());
	}
}
