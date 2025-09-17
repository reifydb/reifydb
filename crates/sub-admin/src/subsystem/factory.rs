// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{interface::Transaction, ioc::IocContainer};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::AdminSubsystem;
use crate::config::AdminConfig;

pub struct AdminSubsystemFactory<T: Transaction> {
	config: AdminConfig,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> AdminSubsystemFactory<T> {
	pub fn new(config: AdminConfig) -> Self {
		Self {
			config,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>> for AdminSubsystemFactory<T> {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;
		let subsystem = AdminSubsystem::new(self.config, engine);
		Ok(Box::new(subsystem))
	}
}
