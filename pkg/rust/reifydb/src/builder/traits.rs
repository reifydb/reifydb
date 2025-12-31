// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::TracingBuilder;

pub trait WithSubsystem: Sized {
	#[cfg(feature = "sub_tracing")]
	fn with_tracing<F>(self, configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static;

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static;

	fn with_subsystem(self, factory: Box<dyn SubsystemFactory<StandardCommandTransaction>>) -> Self;
}
