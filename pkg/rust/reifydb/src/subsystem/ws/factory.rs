// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interceptor::StandardInterceptorBuilder, interface::Transaction,
};
use reifydb_engine::StandardEngine;
use reifydb_network::ws::server::WsConfig;

use super::WsSubsystem;
use crate::{
	context::RuntimeProvider,
	ioc::IocContainer,
	subsystem::{Subsystem, factory::SubsystemFactory},
};

/// Factory for creating WsSubsystem
pub struct WsSubsystemFactory<T: Transaction> {
	config: WsConfig,
	runtime_provider: RuntimeProvider,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> WsSubsystemFactory<T> {
	pub fn new(
		config: WsConfig,
		runtime_provider: RuntimeProvider,
	) -> Self {
		Self {
			config,
			runtime_provider,
			_phantom: std::marker::PhantomData,
		}
	}
}

impl<T: Transaction> SubsystemFactory<T> for WsSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// WS subsystem doesn't need any special interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Box<dyn Subsystem> {
		// Get engine from IoC
		let engine = ioc
			.resolve::<StandardEngine<T>>()
			.expect("StandardEngine must be registered in IoC");

		Box::new(WsSubsystem::new(
			self.config,
			engine,
			&self.runtime_provider,
		))
	}
}
