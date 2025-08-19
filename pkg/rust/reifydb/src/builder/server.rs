// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::{AddToBuilder, StandardInterceptorBuilder},
	interface::Transaction,
};
use reifydb_engine::StandardEngine;
use reifydb_network::ws::server::WsConfig;

use super::DatabaseBuilder;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
use crate::context::{RuntimeProvider, TokioRuntimeProvider};
use crate::{Database, subsystem::SubsystemBuilder};

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptors: StandardInterceptorBuilder<T>,
	subsystem_builders: Vec<SubsystemBuilder>,
	runtime_provider: RuntimeProvider,
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<T: Transaction> ServerBuilder<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		let runtime_provider = RuntimeProvider::Tokio(
			TokioRuntimeProvider::new().expect(
				"Failed to create Tokio runtime for server",
			),
		);

		Self {
			versioned,
			unversioned,
			cdc,
			hooks,
			interceptors: StandardInterceptorBuilder::new(),
			subsystem_builders: Vec::new(),
			runtime_provider,
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: AddToBuilder<T>,
	{
		self.interceptors =
			interceptor.add_to_builder(self.interceptors);
		self
	}

	#[cfg(feature = "sub_ws")]
	pub fn with_ws(mut self, config: WsConfig) -> Self {
		self.subsystem_builders.push(SubsystemBuilder::Ws(config));
		self
	}

	#[cfg(feature = "sub_grpc")]
	pub fn with_grpc(
		mut self,
		config: reifydb_network::grpc::server::GrpcConfig,
	) -> Self {
		self.subsystem_builders.push(SubsystemBuilder::Grpc(config));
		self
	}

	pub fn build(self) -> Database<T> {
		// Create the engine
		let engine = StandardEngine::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
			Box::new(self.interceptors.build()),
		);

		// Build subsystems with the engine
		let mut inner = DatabaseBuilder::new(engine.clone());
		for builder in self.subsystem_builders {
			let subsystem =
				builder.build(&engine, &self.runtime_provider);
			inner = inner.add_boxed_subsystem(subsystem);
		}

		inner.build()
	}
}
