// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interceptor::{InterceptorProvider, StandardInterceptorBuilder},
	interface::Transaction,
};
use reifydb_engine::StandardEngine;
use reifydb_network::grpc::server::GrpcConfig;

use super::GrpcSubsystem;
use crate::{
	context::RuntimeProvider,
	subsystem::{Subsystem, factory::SubsystemFactory},
};

/// Factory for creating GrpcSubsystem
pub struct GrpcSubsystemFactory<T: Transaction> {
	config: GrpcConfig,
	runtime_provider: RuntimeProvider,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> GrpcSubsystemFactory<T> {
	pub fn new(
		config: GrpcConfig,
		runtime_provider: RuntimeProvider,
	) -> Self {
		Self {
			config,
			runtime_provider,
			_phantom: std::marker::PhantomData,
		}
	}
}

impl<T: Transaction> InterceptorProvider<T> for GrpcSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
	) -> StandardInterceptorBuilder<T> {
		// GRPC subsystem doesn't need any special interceptors
		builder
	}
}

impl<T: Transaction> SubsystemFactory<T> for GrpcSubsystemFactory<T> {
	fn create(
		self: Box<Self>,
		engine: StandardEngine<T>,
	) -> Box<dyn Subsystem> {
		Box::new(GrpcSubsystem::new(
			self.config,
			engine,
			&self.runtime_provider,
		))
	}
}
