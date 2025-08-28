// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::{
	Transaction,
	subsystem::{HealthStatus, Subsystem},
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
}

impl<T: Transaction> Subsystem for ServerSubsystem<T> {
	fn name(&self) -> &'static str {
		"server"
	}

	fn start(&mut self) -> reifydb_core::Result<()> {
		if self.server.is_some() {
			return Ok(());
		}

		println!(
			"Starting server on {} with WebSocket and HTTP protocol support",
			self.config.bind_addr
		);

		let mut server = ProtocolServer::new(
			self.config.clone(),
			self.engine.clone(),
		);

		// Configure protocol handlers
		server.with_websocket().with_http();

		server.start().map_err(|e| {
            reifydb_core::error!(reifydb_core::result::error::diagnostic::internal::internal(
                format!("Failed to start server: {:?}", e)
            ))
        })?;

		self.server = Some(server);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		if let Some(mut server) = self.server.take() {
			println!("Stopping server");
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
				description: "Server is not running"
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
