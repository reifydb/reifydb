// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingBuilder;
use reifydb_sub_worker::WorkerBuilder;

pub trait WithSubsystem: Sized {
	#[cfg(feature = "sub_logging")]
	fn with_logging<F>(self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static;

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static;

	fn with_worker<F>(self, configurator: F) -> Self
	where
		F: FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static;

	fn with_subsystem(self, factory: Box<dyn SubsystemFactory<StandardCommandTransaction>>) -> Self;
}
