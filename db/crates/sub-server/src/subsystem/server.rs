// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::{
	Transaction,
	subsystem::{HealthStatus, Subsystem},
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_engine::StandardEngine;

use crate::{config::ServerConfig, core::ProtocolServer};

/// Server subsystem that supports WebSocket and HTTP protocols
pub struct ServerSubsystem<T: Transaction> {
	config: ServerConfig,
	server: Option<ProtocolServer<T>>,
	engine: StandardEngine<T>,
}

impl<T: Transaction> ServerSubsystem<T> {
	pub fn new(config: ServerConfig, engine: StandardEngine<T>) -> Self {
		Self {
			config,
			server: None,
			engine,
		}
	}

	/// Get the actual bound port of the server
	pub fn port(&self) -> Option<u16> {
		self.server.as_ref().and_then(|s| s.port())
	}
}

impl<T: Transaction> Subsystem for ServerSubsystem<T> {
	fn name(&self) -> &'static str {
		"server"
	}

	fn start(&mut self) -> reifydb_type::Result<()> {
		if self.server.is_some() {
			return Ok(());
		}

		// Starting server

		let mut server = ProtocolServer::new(self.config.clone(), self.engine.clone());

		// Configure protocol handlers
		server.with_websocket().with_http();

		server.start().map_err(|e| {
			reifydb_type::error!(reifydb_type::diagnostic::internal::internal(format!(
				"Failed to start server: {:?}",
				e
			)))
		})?;

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

impl<T: Transaction> HasVersion for ServerSubsystem<T> {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-server".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Network protocol server subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}
