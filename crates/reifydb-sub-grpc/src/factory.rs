// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{
		Transaction,
		subsystem::{Subsystem, SubsystemFactory},
	},
	ioc::IocContainer,
	transaction::StandardCommandTransaction,
};
use reifydb_engine::StandardEngine;
use reifydb_network::grpc::server::GrpcConfig;

use super::GrpcSubsystem;

/// Factory for creating GrpcSubsystem
pub struct GrpcSubsystemFactory<T: Transaction> {
	config: GrpcConfig,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> GrpcSubsystemFactory<T> {
	pub fn new(config: GrpcConfig) -> Self {
		Self {
			config,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>> for GrpcSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<StandardCommandTransaction<T>> {
		// GRPC subsystem doesn't need any special interceptors
		builder
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		Ok(Box::new(GrpcSubsystem::new(self.config, engine)))
	}
}
