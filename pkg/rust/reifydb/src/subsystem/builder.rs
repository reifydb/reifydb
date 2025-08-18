// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;

use super::Subsystem;
#[cfg(feature = "sub_grpc")]
use super::grpc::GrpcSubsystem;
#[cfg(feature = "sub_ws")]
use super::ws::WsSubsystem;
use crate::context::RuntimeProvider;
#[cfg(feature = "sub_grpc")]
use crate::network::grpc::server::GrpcConfig;
#[cfg(feature = "sub_ws")]
use crate::network::ws::server::WsConfig;

/// Builder for creating subsystems
pub enum SubsystemBuilder {
	#[cfg(feature = "sub_ws")]
	Ws(WsConfig),
	#[cfg(feature = "sub_grpc")]
	Grpc(GrpcConfig),
}

impl SubsystemBuilder {
	/// Build the subsystem with the provided engine and runtime
	#[allow(unreachable_patterns)]
	pub fn build<T: Transaction>(
		self,
		engine: &StandardEngine<T>,
		runtime_provider: &RuntimeProvider,
	) -> Box<dyn Subsystem> {
		match self {
			#[cfg(feature = "sub_ws")]
			SubsystemBuilder::Ws(config) => Box::new(WsSubsystem::new(
				config,
				engine.clone(),
				runtime_provider,
			)),
			#[cfg(feature = "sub_grpc")]
			SubsystemBuilder::Grpc(config) => Box::new(GrpcSubsystem::new(
				config,
				engine.clone(),
				runtime_provider,
			)),
		}
	}
}
