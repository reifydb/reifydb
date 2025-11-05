// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{HealthStatus, Subsystem};

use crate::{config::AdminConfig, server::AdminServer};

pub struct AdminSubsystem {
	config: AdminConfig,
	server: Option<AdminServer>,
	engine: StandardEngine,
}

impl AdminSubsystem {
	pub fn new(config: AdminConfig, engine: StandardEngine) -> Self {
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

impl Subsystem for AdminSubsystem {
	fn name(&self) -> &'static str {
		"sub-admin"
	}

	fn start(&mut self) -> reifydb_type::Result<()> {
		if !self.config.enabled {
			return Ok(());
		}

		if self.server.is_some() {
			return Ok(());
		}

		let mut server = AdminServer::new(self.config.clone(), self.engine.clone());

		server.start().map_err(|e| {
			reifydb_type::error!(reifydb_type::diagnostic::internal::internal(format!(
				"Failed to start admin server: {:?}",
				e
			)))
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
				description: "Admin server is not running".to_string(),
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

impl HasVersion for AdminSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-admin".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Web administration interface subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
