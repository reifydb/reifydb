// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone, PartialEq)]
pub enum Route {
	// API endpoints
	Health,
	GetConfig,
	UpdateConfig,
	Execute,
	Metrics,
	WebSocket,
	Login,
	Logout,
	AuthStatus,

	// Static files
	ServeIndex,
	ServeStatic(String),

	// Error
	NotFound,
}

pub struct Router;

impl Router {
	pub fn route(method: &str, path: &str) -> Route {
		match (method, path) {
			// API endpoints under /v1/
			("GET", "/v1/health") => Route::Health,
			("GET", "/v1/config") => Route::GetConfig,
			("PUT", "/v1/config") => Route::UpdateConfig,
			("POST", "/v1/execute") => Route::Execute,
			("GET", "/v1/metrics") => Route::Metrics,
			("GET", "/v1/ws") => Route::WebSocket,
			("POST", "/v1/auth/login") => Route::Login,
			("POST", "/v1/auth/logout") => Route::Logout,
			("GET", "/v1/auth/status") => Route::AuthStatus,

			// React app routes - serve at root
			("GET", "/") => Route::ServeIndex,
			("GET", p) if p.starts_with("/assets/") => Route::ServeStatic(p.to_string()),
			// SPA fallback - any other GET not under /v1/
			("GET", p) if !p.starts_with("/v1/") => Route::ServeIndex,

			_ => Route::NotFound,
		}
	}
}
