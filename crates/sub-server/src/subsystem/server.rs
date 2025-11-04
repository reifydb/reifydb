// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{HealthStatus, SchedulerService, Subsystem};
use reifydb_type::{diagnostic::internal::internal, error};

use crate::{config::ServerConfig, core::ProtocolServer};

/// Server subsystem that supports WebSocket and HTTP protocols
pub struct ServerSubsystem {
	config: ServerConfig,
	server: Option<ProtocolServer>,
	engine: StandardEngine,
	scheduler: SchedulerService,
}

impl ServerSubsystem {
	pub fn new(config: ServerConfig, engine: StandardEngine, scheduler: SchedulerService) -> Self {
		Self {
			config,
			server: None,
			engine,
			scheduler,
		}
	}

	/// Get the actual bound port of the server
	pub fn port(&self) -> Option<u16> {
		self.server.as_ref().and_then(|s| s.port())
	}
}

impl Subsystem for ServerSubsystem {
	fn name(&self) -> &'static str {
		"server"
	}

	fn start(&mut self) -> reifydb_type::Result<()> {
		if self.server.is_some() {
			return Ok(());
		}

		let mut server = ProtocolServer::new(self.config.clone(), self.engine.clone(), self.scheduler.clone());
		server.with_websocket().with_http();
		server.start().map_err(|e| error!(internal(format!("Failed to start server: {:?}", e))))?;

		self.server = Some(server);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_type::Result<()> {
		if let Some(mut server) = self.server.take() {
			// Stopping server
			server.stop();
		}
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.server.as_ref().map_or(false, |s| s.is_running())
	}

	fn health_status(&self) -> HealthStatus {
		if self.is_running() {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Server is not running".to_string(),
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

impl HasVersion for ServerSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-server".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Network protocol server subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
