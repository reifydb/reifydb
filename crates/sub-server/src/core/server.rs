// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_engine::StandardEngine;
use reifydb_sub_api::SchedulerService;

use super::Listener;
use crate::{
	config::ServerConfig,
	protocols::{HttpHandler, ProtocolError, ProtocolHandler, ProtocolResult, WebSocketHandler},
};

/// Multi-protocol server that can handle WebSocket and HTTP protocols
pub struct ProtocolServer {
	config: ServerConfig,
	websocket: Option<WebSocketHandler>,
	http: Option<HttpHandler>,
	listener: Option<Listener>,
	engine: StandardEngine,
	scheduler: SchedulerService,
}

impl ProtocolServer {
	pub fn new(config: ServerConfig, engine: StandardEngine, scheduler: SchedulerService) -> Self {
		Self {
			config,
			websocket: None,
			http: None,
			listener: None,
			engine,
			scheduler,
		}
	}

	/// Add WebSocket protocol support
	pub fn with_websocket(&mut self) -> &mut Self {
		self.websocket = Some(WebSocketHandler::new());
		self
	}

	/// Add HTTP protocol support
	pub fn with_http(&mut self) -> &mut Self {
		self.http = Some(HttpHandler::new());
		self
	}

	/// Start the multi-protocol server
	pub fn start(&mut self) -> ProtocolResult<()> {
		if self.listener.is_some() {
			return Ok(());
		}

		let enabled_protocols = self.get_enabled_protocols();
		if enabled_protocols.is_empty() {
			return Err(ProtocolError::Custom(
				"No protocols configured. Use with_websocket() or with_http()".to_string(),
			));
		}

		self.listener = Some(Listener::new(
			self.config.clone(),
			self.engine.clone(),
			self.scheduler.clone(),
			self.websocket.clone(),
			self.http.clone(),
		));
		Ok(())
	}

	/// Stop the server
	pub fn stop(&mut self) {
		if let Some(worker_pool) = self.listener.take() {
			worker_pool.stop();
		}
	}

	/// Detect which protocol should handle a connection
	pub fn detect_protocol(&self, buffer: &[u8]) -> Option<&str> {
		// Check protocols in order of likelihood/preference
		if let Some(ref websocket) = self.websocket {
			if <WebSocketHandler as ProtocolHandler>::can_handle(websocket, buffer) {
				return Some("ws");
			}
		}

		if let Some(ref http) = self.http {
			if <HttpHandler as ProtocolHandler>::can_handle(http, buffer) {
				return Some("http");
			}
		}

		None
	}

	/// Get WebSocket handler if enabled
	pub fn websocket_handler(&self) -> Option<&WebSocketHandler> {
		self.websocket.as_ref()
	}

	/// Get HTTP handler if enabled
	pub fn http_handler(&self) -> Option<&HttpHandler> {
		self.http.as_ref()
	}

	/// Check if server is running
	pub fn is_running(&self) -> bool {
		self.listener.is_some()
	}

	/// Get server configuration
	pub fn config(&self) -> &ServerConfig {
		&self.config
	}

	/// Get list of enabled protocols
	pub fn get_enabled_protocols(&self) -> Vec<String> {
		let mut protocols = Vec::new();

		if self.websocket.is_some() {
			protocols.push("WebSocket".to_string());
		}
		if self.http.is_some() {
			protocols.push("HTTP".to_string());
		}

		protocols
	}

	/// Get list of enabled protocol names
	pub fn protocols(&self) -> Vec<&str> {
		let mut protocols = Vec::new();

		if self.websocket.is_some() {
			protocols.push("ws");
		}
		if self.http.is_some() {
			protocols.push("http");
		}

		protocols
	}

	/// Get the actual bound port of the server
	pub fn port(&self) -> Option<u16> {
		self.listener.as_ref().map(|pool| pool.port())
	}
}
