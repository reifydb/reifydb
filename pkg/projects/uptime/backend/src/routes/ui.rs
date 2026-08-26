// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	extract::{Path, Request},
	http::{StatusCode, header},
	response::{IntoResponse, Response},
};

use crate::assets::get_embedded_file;

pub async fn serve_static(Path(path): Path<String>) -> Response {
	match get_embedded_file(&format!("assets/{path}")) {
		Some(file) => ([(header::CONTENT_TYPE, file.mime_type)], file.content).into_response(),
		None => StatusCode::NOT_FOUND.into_response(),
	}
}

pub async fn serve_index(req: Request) -> Response {
	let path = req.uri().path();
	if path.starts_with("/api/") || path.starts_with("/db/") || path == "/health" {
		return (StatusCode::NOT_FOUND, "not found").into_response();
	}
	if let Some(file) = get_embedded_file(path) {
		return ([(header::CONTENT_TYPE, file.mime_type)], file.content).into_response();
	}
	match get_embedded_file("index.html") {
		Some(index) => ([(header::CONTENT_TYPE, index.mime_type)], index.content).into_response(),
		None => (StatusCode::NOT_FOUND, "web app not built").into_response(),
	}
}
