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
};
use reifydb_engine::StandardEngine;
use reifydb_network::ws::server::WsConfig;

use super::WsSubsystem;
use crate::context::RuntimeProvider;

/// Factory for creating WsSubsystem
pub struct WsSubsystemFactory<T: Transaction> {
	config: WsConfig,
	runtime_provider: RuntimeProvider,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> WsSubsystemFactory<T> {
	pub fn new(
		config: WsConfig,
		runtime_provider: RuntimeProvider,
	) -> Self {
		Self {
			config,
			runtime_provider,
			_phantom: PhantomData,
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

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> crate::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		Ok(Box::new(WsSubsystem::new(
			self.config,
			engine,
			&self.runtime_provider,
		)))
	}
}
