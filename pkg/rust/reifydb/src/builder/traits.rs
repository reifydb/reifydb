// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Transaction, subsystem::SubsystemFactory};
use reifydb_engine::StandardCommandTransaction;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingBuilder;

pub trait WithSubsystem<T: Transaction>: Sized {
	#[cfg(feature = "sub_logging")]
	fn with_logging<F>(self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static;

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder<T>) -> FlowBuilder<T> + Send + 'static;

	fn with_subsystem(self, factory: Box<dyn SubsystemFactory<StandardCommandTransaction<T>>>) -> Self;
}
