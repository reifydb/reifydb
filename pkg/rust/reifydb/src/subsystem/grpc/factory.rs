// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::interface::subsystem::{Subsystem, SubsystemFactory};
use reifydb_core::{
    interceptor::StandardInterceptorBuilder, interface::Transaction,
    ioc::IocContainer,
};
use reifydb_engine::StandardEngine;
use reifydb_network::grpc::server::GrpcConfig;

use super::GrpcSubsystem;
use crate::context::RuntimeProvider;

/// Factory for creating GrpcSubsystem
pub struct GrpcSubsystemFactory<T: Transaction> {
	config: GrpcConfig,
	runtime_provider: RuntimeProvider,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> GrpcSubsystemFactory<T> {
	pub fn new(
		config: GrpcConfig,
		runtime_provider: RuntimeProvider,
	) -> Self {
		Self {
			config,
			runtime_provider,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> SubsystemFactory<T> for GrpcSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// GRPC subsystem doesn't need any special interceptors
		builder
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> crate::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		Ok(Box::new(GrpcSubsystem::new(
			self.config,
			engine,
			&self.runtime_provider,
		)))
	}
}
