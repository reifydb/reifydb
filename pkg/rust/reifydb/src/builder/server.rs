// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{hook::Hooks, interface::Transaction};
use reifydb_engine::StandardEngine;
use reifydb_network::ws::server::WsConfig;

use super::DatabaseBuilder;
#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
use crate::context::{RuntimeProvider, TokioRuntimeProvider};
#[cfg(feature = "sub_grpc")]
use crate::subsystem::GrpcSubsystem;
#[cfg(feature = "sub_ws")]
use crate::subsystem::WsSubsystem;
use crate::{Database, hook::WithHooks};

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub struct ServerBuilder<T: Transaction> {
	inner: DatabaseBuilder<T>,
	engine: StandardEngine<T>,
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
		let engine = StandardEngine::new(
			versioned,
			unversioned,
			cdc,
			hooks.clone(),
		)
		.unwrap();
		let inner = DatabaseBuilder::new(engine.clone());
		let runtime_provider = RuntimeProvider::Tokio(
			TokioRuntimeProvider::new().expect(
				"Failed to create Tokio runtime for server",
			),
		);
		Self {
			inner,
			engine,
			runtime_provider,
		}
	}

	#[cfg(feature = "sub_ws")]
	pub fn with_ws(mut self, config: WsConfig) -> Self {
		let subsystem = WsSubsystem::new(
			config,
			self.engine.clone(),
			&self.runtime_provider,
		);
		self.inner = self.inner.add_subsystem(subsystem);
		self
	}

	#[cfg(feature = "sub_grpc")]
	pub fn with_grpc(
		mut self,
		config: reifydb_network::grpc::server::GrpcConfig,
	) -> Self {
		let subsystem = GrpcSubsystem::new(
			config,
			self.engine.clone(),
			&self.runtime_provider,
		);
		self.inner = self.inner.add_subsystem(subsystem);
		self
	}

	pub fn build(self) -> Database<T> {
		self.inner.build()
	}
}

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
impl<T: Transaction> WithHooks<T> for ServerBuilder<T> {
	fn engine(&self) -> &StandardEngine<T> {
		&self.engine
	}
}
