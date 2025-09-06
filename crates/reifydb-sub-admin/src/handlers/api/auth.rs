// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_network::{HttpRequest, HttpResponse};
use serde_json::{Value, json};

use crate::config::AdminConfig;

pub fn handle_login(
	config: &AdminConfig,
	request: HttpRequest,
) -> HttpResponse {
	if !config.auth_required {
		return HttpResponse::ok().with_json(
			r#"{"success":true,"message":"Auth not required"}"#,
		);
	}

	let body_str = String::from_utf8_lossy(&request.body);
	let json_body: Result<Value, _> = serde_json::from_str(&body_str);

	match json_body {
		Ok(json) => {
			if let Some(token) =
				json.get("token").and_then(|t| t.as_str())
			{
				if Some(token.to_string()) == config.auth_token
				{
					// TODO: Generate session token
					let response = json!({
						"success": true,
						"session_token": "temp_session_token"
					});
					HttpResponse::ok().with_json(
						&response.to_string(),
					)
				} else {
					HttpResponse::bad_request().with_json(
						r#"{"error":"Invalid token"}"#,
					)
				}
			} else {
				HttpResponse::bad_request().with_json(
					r#"{"error":"Missing 'token' field"}"#,
				)
			}
		}
		Err(e) => HttpResponse::bad_request().with_json(&format!(
			r#"{{"error":"Invalid JSON: {}"}}"#,
			e
		)),
	}
}

pub fn handle_logout() -> HttpResponse {
	HttpResponse::ok()
		.with_json(r#"{"success":true,"message":"Logged out"}"#)
}

pub fn handle_auth_status(
	config: &AdminConfig,
	_request: HttpRequest,
) -> HttpResponse {
	let status = json!({
		"auth_required": config.auth_required,
		"authenticated": !config.auth_required, // TODO: Check actual auth status
	});

	HttpResponse::ok().with_json(&status.to_string())
}
