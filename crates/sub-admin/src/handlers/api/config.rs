// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_network::{HttpRequest, HttpResponse};
use serde_json::json;

use crate::config::AdminConfig;

pub fn handle_get_config(config: &AdminConfig) -> HttpResponse {
	let config_json = json!({
		"enabled": config.enabled,
		"port": config.port,
		"bind_address": config.bind_address,
		"auth_required": config.auth_required,
		// Don't expose auth token
	});

	HttpResponse::ok().with_json(&config_json.to_string())
}

pub fn handle_update_config(_config: &AdminConfig, _request: HttpRequest) -> HttpResponse {
	// TODO: Implement configuration update logic
	HttpResponse::ok().with_json(r#"{"message":"Configuration update not yet implemented"}"#)
}
