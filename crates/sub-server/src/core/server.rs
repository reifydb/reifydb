// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_engine::StandardEngine;
use reifydb_sub_api::SchedulerService;

use super::Listener;
use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, ProtocolError, ProtocolResult, WebSocketHandler},
};

/// Multi-protocol server that can handle WebSocket and HTTP protocols on separate addresses
pub struct ProtocolServer {
	config: ServerConfig,
	http_listener: Option<Listener>,
	ws_listener: Option<Listener>,
	engine: StandardEngine,
	scheduler: SchedulerService,
}

impl ProtocolServer {
	pub fn new(config: ServerConfig, engine: StandardEngine, scheduler: SchedulerService) -> Self {
		Self {
			config,
			http_listener: None,
			ws_listener: None,
			engine,
			scheduler,
		}
	}

	/// Start the server(s) based on configuration
	pub fn start(&mut self) -> ProtocolResult<()> {
		// Already started
		if self.http_listener.is_some() || self.ws_listener.is_some() {
			return Ok(());
		}

		let has_http = self.config.http_bind_addr.is_some();
		let has_ws = self.config.ws_bind_addr.is_some();

		if !has_http && !has_ws {
			return Err(ProtocolError::Custom(
				"No server configured. Set http_bind_addr or ws_bind_addr".to_string(),
			));
		}

		// Start HTTP listener if configured
		if let Some(ref http_addr) = self.config.http_bind_addr {
			self.http_listener = Some(Listener::new_for_address(
				http_addr,
				&self.config,
				self.engine.clone(),
				self.scheduler.clone(),
				None, // No WS handler for HTTP listener
				Some(HttpHandler::new()),
			));
		}

		// Start WS listener if configured
		if let Some(ref ws_addr) = self.config.ws_bind_addr {
			self.ws_listener = Some(Listener::new_for_address(
				ws_addr,
				&self.config,
				self.engine.clone(),
				self.scheduler.clone(),
				Some(WebSocketHandler::new()),
				None, // No HTTP handler for WS listener
			));
		}

		Ok(())
	}

	/// Stop the server(s)
	pub fn stop(&mut self) {
		if let Some(listener) = self.http_listener.take() {
			listener.stop();
		}
		if let Some(listener) = self.ws_listener.take() {
			listener.stop();
		}
	}

	/// Check if any server is running
	pub fn is_running(&self) -> bool {
		self.http_listener.is_some() || self.ws_listener.is_some()
	}

	/// Get server configuration
	pub fn config(&self) -> &ServerConfig {
		&self.config
	}

	/// Get list of enabled protocols
	pub fn get_enabled_protocols(&self) -> Vec<String> {
		let mut protocols = Vec::new();

		if self.config.ws_bind_addr.is_some() {
			protocols.push("WebSocket".to_string());
		}
		if self.config.http_bind_addr.is_some() {
			protocols.push("HTTP".to_string());
		}

		protocols
	}

	/// Get list of enabled protocol names
	pub fn protocols(&self) -> Vec<&str> {
		let mut protocols = Vec::new();

		if self.config.ws_bind_addr.is_some() {
			protocols.push("ws");
		}
		if self.config.http_bind_addr.is_some() {
			protocols.push("http");
		}

		protocols
	}

	/// Get the actual bound port of the HTTP server
	pub fn http_port(&self) -> Option<u16> {
		self.http_listener.as_ref().map(|l| l.port())
	}

	/// Get the actual bound port of the WebSocket server
	pub fn ws_port(&self) -> Option<u16> {
		self.ws_listener.as_ref().map(|l| l.port())
	}
}
