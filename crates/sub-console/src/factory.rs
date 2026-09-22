// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{error::CoreError, util::ioc::IocContainer};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server_ws::acceptor::WsStreamAcceptor;
use reifydb_value::{Result, error::Error};
use tokio::runtime::Handle;

use crate::{
	config::{ConsoleConfig, ConsoleConfigurator},
	session::SessionConfig,
	subsystem::ConsoleSubsystem,
};

pub struct ConsoleSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> ConsoleConfig + Send>,
}

impl ConsoleSubsystemFactory {
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(ConsoleConfigurator) -> ConsoleConfigurator + Send + 'static,
	{
		Self {
			config_fn: Box::new(move || configurator(ConsoleConfigurator::new()).configure()),
		}
	}
}

impl SubsystemFactory for ConsoleSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let config = (self.config_fn)();

		let acceptor = ioc
			.try_resolve::<WsStreamAcceptor>()
			.ok_or_else(|| init_failed("with_console requires with_ws"))?;
		let handle = ioc.resolve::<Handle>()?;

		let address = config.address.ok_or_else(|| init_failed("with_console requires address"))?;
		let token = config.token.ok_or_else(|| init_failed("with_console requires token"))?;

		let session = SessionConfig {
			address,
			token,
			fingerprint_path: config.fingerprint_path,
		};
		Ok(Box::new(ConsoleSubsystem::new(session, acceptor, handle)))
	}
}

fn init_failed(reason: &str) -> Error {
	CoreError::SubsystemInitFailed {
		subsystem: "console".to_string(),
		reason: reason.to_string(),
	}
	.into()
}
