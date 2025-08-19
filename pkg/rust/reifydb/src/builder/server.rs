// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::{AddToBuilder, StandardInterceptorBuilder},
	interface::Transaction,
};
#[cfg(feature = "sub_grpc")]
use reifydb_network::grpc::server::GrpcConfig;
use reifydb_network::ws::server::WsConfig;

use super::DatabaseBuilder;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
use crate::context::{RuntimeProvider, TokioRuntimeProvider};
#[cfg(feature = "sub_grpc")]
use crate::subsystem::GrpcSubsystemFactory;
#[cfg(feature = "sub_ws")]
use crate::subsystem::WsSubsystemFactory;
use crate::{Database, subsystem::SubsystemFactory};

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptors: StandardInterceptorBuilder<T>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory<T>>>,
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
			subsystem_factories: Vec::new(),
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
		let factory = WsSubsystemFactory::new(
			config,
			self.runtime_provider.clone(),
		);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(feature = "sub_grpc")]
	pub fn with_grpc(mut self, config: GrpcConfig) -> Self {
		let factory = GrpcSubsystemFactory::new(
			config,
			self.runtime_provider.clone(),
		);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database<T>> {
		let mut database_builder = DatabaseBuilder::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
		)
		.with_interceptor_builder(self.interceptors);

		// Add all subsystem factories
		for factory in self.subsystem_factories {
			database_builder =
				database_builder.add_subsystem_factory(factory);
		}

		database_builder.build()
	}
}
