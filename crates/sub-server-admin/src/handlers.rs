// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use axum::{
	Json,
	body::Body,
	extract::{Path, State},
	http::{Response, StatusCode, header},
	response::IntoResponse,
};
use reifydb_core::actors::admin::{AdminExecuteResponse, AdminLoginResponse, AdminMessage};
use reifydb_runtime::actor::reply::reply_channel;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{assets, state::AdminState};

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
	pub token: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
	pub success: bool,
	pub message: Option<String>,
	pub session_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AuthStatusResponse {
	pub auth_required: bool,
	pub authenticated: bool,
}

pub async fn handle_login(State(state): State<AdminState>, Json(request): Json<LoginRequest>) -> impl IntoResponse {
	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(AdminMessage::Login {
			token: request.token,
			reply,
		})
		.ok();

	let response = receiver.recv().await.unwrap();

	match response {
		AdminLoginResponse::AuthNotRequired => (
			StatusCode::OK,
			Json(LoginResponse {
				success: true,
				message: Some("Auth not required".to_string()),
				session_token: None,
			}),
		),
		AdminLoginResponse::Success {
			session_token,
		} => (
			StatusCode::OK,
			Json(LoginResponse {
				success: true,
				message: None,
				session_token: Some(session_token),
			}),
		),
		AdminLoginResponse::InvalidToken => (
			StatusCode::BAD_REQUEST,
			Json(LoginResponse {
				success: false,
				message: Some("Invalid token".to_string()),
				session_token: None,
			}),
		),
	}
}

pub async fn handle_logout(State(state): State<AdminState>) -> impl IntoResponse {
	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(AdminMessage::Logout {
			reply,
		})
		.ok();

	let _response = receiver.recv().await.unwrap();

	(
		StatusCode::OK,
		Json(json!({
			"success": true,
			"message": "Logged out"
		})),
	)
}

pub async fn handle_auth_status(State(state): State<AdminState>) -> impl IntoResponse {
	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(AdminMessage::AuthStatus {
			reply,
		})
		.ok();

	let response = receiver.recv().await.unwrap();

	(
		StatusCode::OK,
		Json(AuthStatusResponse {
			auth_required: response.auth_required,
			authenticated: response.authenticated,
		}),
	)
}

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
	pub query: String,
}

pub async fn handle_execute(State(state): State<AdminState>, Json(request): Json<ExecuteRequest>) -> impl IntoResponse {
	let (reply, receiver) = reply_channel();
	let (actor_ref, _handle) = state.spawn_actor();
	actor_ref
		.send(AdminMessage::Execute {
			query: request.query.clone(),
			reply,
		})
		.ok();

	let response = receiver.recv().await.unwrap();

	match response {
		AdminExecuteResponse::Success {
			message,
		} => (
			StatusCode::OK,
			Json(json!({
				"success": true,
				"message": message
			})),
		),
		AdminExecuteResponse::NotImplemented => (
			StatusCode::OK,
			Json(json!({
				"success": true,
				"message": "Query execution not yet implemented",
				"query": request.query
			})),
		),
		AdminExecuteResponse::Error(err) => (
			StatusCode::INTERNAL_SERVER_ERROR,
			Json(json!({
				"success": false,
				"message": err
			})),
		),
	}
}

const FALLBACK_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>ReifyDB Admin</title>
    <style>
        body { font-family: system-ui; max-width: 800px; margin: 50px auto; padding: 20px; }
        .error { background: #fee; padding: 20px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>ReifyDB Admin Console</h1>
    <div class="error">
        <p>React app not found. Please build the webapp first.</p>
    </div>
</body>
</html>"#;

pub async fn serve_index() -> impl IntoResponse {
	if let Some(file) = assets::get_embedded_file("index.html") {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, file.mime_type)
			.body(Body::from(file.content.to_vec()))
			.unwrap()
	} else {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, "text/html")
			.body(Body::from(FALLBACK_HTML))
			.unwrap()
	}
}

pub async fn serve_static(Path(path): Path<String>) -> impl IntoResponse {
	let clean_path = path.strip_prefix('/').unwrap_or(&path);
	let full_path = format!("assets/{}", clean_path);

	if let Some(file) = assets::get_embedded_file(&full_path) {
		Response::builder()
			.status(StatusCode::OK)
			.header(header::CONTENT_TYPE, file.mime_type)
			.header(header::CACHE_CONTROL, "public, max-age=31536000")
			.body(Body::from(file.content.to_vec()))
			.unwrap()
	} else {
		Response::builder()
			.status(StatusCode::NOT_FOUND)
			.header(header::CONTENT_TYPE, "text/plain")
			.body(Body::from(format!("Static file not found: {}", full_path)))
			.unwrap()
	}
}
