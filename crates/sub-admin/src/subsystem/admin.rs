// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::{
	Transaction,
	subsystem::{HealthStatus, Subsystem},
	version::{ComponentKind, HasVersion, SystemVersion},
};
use reifydb_engine::StandardEngine;

use crate::{config::AdminConfig, server::AdminServer};

pub struct AdminSubsystem<T: Transaction> {
	config: AdminConfig,
	server: Option<AdminServer<T>>,
	engine: StandardEngine<T>,
}

impl<T: Transaction> AdminSubsystem<T> {
	pub fn new(config: AdminConfig, engine: StandardEngine<T>) -> Self {
		Self {
			config,
			server: None,
			engine,
		}
	}

	pub fn port(&self) -> u16 {
		self.config.port
	}
}

impl<T: Transaction> Subsystem for AdminSubsystem<T> {
	fn name(&self) -> &'static str {
		"admin"
	}

	fn start(&mut self) -> reifydb_type::Result<()> {
		if !self.config.enabled {
			return Ok(());
		}

		if self.server.is_some() {
			return Ok(());
		}

		let mut server = AdminServer::new(
			self.config.clone(),
			self.engine.clone(),
		);

		server.start().map_err(|e| {
			reifydb_type::error!(
				reifydb_type::diagnostic::internal::internal(
					format!(
						"Failed to start admin server: {:?}",
						e
					)
				)
			)
		})?;

		self.server = Some(server);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_type::Result<()> {
		if let Some(mut server) = self.server.take() {
			server.stop();
		}
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.server.as_ref().map_or(false, |s| s.is_running())
	}

	fn health_status(&self) -> HealthStatus {
		if !self.config.enabled {
			return HealthStatus::Healthy;
		}

		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Admin server is not running"
					.to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl<T: Transaction> HasVersion for AdminSubsystem<T> {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-admin".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Web administration interface subsystem"
				.to_string(),
			kind: ComponentKind::Subsystem,
		}
	}
}
