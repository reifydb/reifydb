// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::Any;

use reifydb_core::interface::{
	Transaction,
	subsystem::{HealthStatus, Subsystem},
};
use reifydb_engine::StandardEngine;

use crate::{config::ServerConfig, server::WebSocketServer};

/// High-performance WebSocket server subsystem
pub struct ServerSubsystem<T: Transaction> {
	config: ServerConfig,
	server: Option<WebSocketServer<T>>,
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
			"Starting WebSocket server on {}",
			self.config.bind_addr
		);

		let mut server = WebSocketServer::new(
			self.config.clone(),
			self.engine.clone(),
		);
		server.start();

		self.server = Some(server);
		Ok(())
	}

	fn shutdown(&mut self) -> reifydb_core::Result<()> {
		if let Some(mut server) = self.server.take() {
			println!("Stopping WebSocket server");
			server.stop();
		}
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.server.is_some()
	}

	fn health_status(&self) -> HealthStatus {
		if self.server.is_some() {
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
