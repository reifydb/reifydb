// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use axum::{
	Json,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use reifydb::Error;
use serde_json::json;
use tracing::error;

#[derive(Debug)]
pub enum ApiError {
	Unauthorized,
	NotFound,
	Conflict(String),
	Validation(String),
	Internal(String),
}

impl ApiError {
	pub fn internal(context: &str, detail: impl Display) -> Self {
		error!("{context}: {detail}");
		ApiError::Internal("internal error".to_string())
	}
}

impl From<Error> for ApiError {
	fn from(err: Error) -> Self {
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
