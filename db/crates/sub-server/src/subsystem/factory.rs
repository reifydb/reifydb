// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	interface::{
		Transaction,
		subsystem::{Subsystem, SubsystemFactory},
	},
	ioc::IocContainer,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};

use crate::{config::ServerConfig, subsystem::ServerSubsystem};

/// Factory for creating server subsystem instances
pub struct ServerSubsystemFactory<T: Transaction> {
	config: ServerConfig,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> ServerSubsystemFactory<T> {
	/// Create a new server subsystem factory with the given configuration
	pub fn new(config: ServerConfig) -> Self {
		Self {
			config,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>>
	for ServerSubsystemFactory<T>
{
	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;
		let subsystem = ServerSubsystem::new(self.config, engine);
		Ok(Box::new(subsystem))
	}
}
