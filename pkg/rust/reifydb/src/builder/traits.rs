// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;

pub trait WithSubsystem: Sized {
	#[cfg(feature = "sub_tracing")]
	fn with_tracing<F>(self, configurator: F) -> Self
	where
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static;

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(self, configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static;

	#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
	fn with_replication<F, C>(self, configurator: F) -> Self
	where
		F: FnOnce(ReplicationConfigurator) -> C + Send + 'static,
		C: Into<ReplicationConfig> + 'static;

	fn with_subsystem(self, factory: Box<dyn SubsystemFactory>) -> Self;
}
