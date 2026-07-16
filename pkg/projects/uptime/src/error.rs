// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	Json,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
	Unauthorized,
	NotFound,
	Conflict(String),
	Validation(String),
	Internal(String),
}

impl ApiError {
	pub fn internal(context: &str, detail: impl std::fmt::Display) -> Self {
		tracing::error!("{context}: {detail}");
		ApiError::Internal("internal error".to_string())
	}
}

impl From<reifydb::Error> for ApiError {
	fn from(err: reifydb::Error) -> Self {
		ApiError::internal("database error", err)
	}
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		let (status, message) = match self {
			ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
			ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
			ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
			ApiError::Validation(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
			ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
		};
		(status, Json(json!({ "error": message }))).into_response()
	}
}
