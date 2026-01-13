// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Application state shared across admin request handler.

use std::time::Duration;

use reifydb_engine::StandardEngine;

/// Shared application state for admin handler.
///
/// This struct is cloneable and cheap to clone since `StandardEngine` uses
/// `Arc` internally.
#[derive(Clone)]
pub struct AdminState {
	engine: StandardEngine,
	max_connections: usize,
	request_timeout: Duration,
	auth_required: bool,
	auth_token: Option<String>,
}

impl AdminState {
	/// Create a new AdminState.
	pub fn new(
		engine: StandardEngine,
		max_connections: usize,
		request_timeout: Duration,
		auth_required: bool,
		auth_token: Option<String>,
	) -> Self {
		Self {
			engine,
			max_connections,
			request_timeout,
			auth_required,
			auth_token,
		}
	}

	/// Get a reference to the database engine.
	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	/// Get a clone of the database engine.
	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	/// Get the maximum connections.
	#[inline]
	pub fn max_connections(&self) -> usize {
		self.max_connections
	}

	/// Get the request timeout.
	#[inline]
	pub fn request_timeout(&self) -> Duration {
		self.request_timeout
	}

	/// Check if authentication is required.
	#[inline]
	pub fn auth_required(&self) -> bool {
		self.auth_required
	}

	/// Get the auth token (if set).
	#[inline]
	pub fn auth_token(&self) -> Option<&str> {
		self.auth_token.as_deref()
	}
}
