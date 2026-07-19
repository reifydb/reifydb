// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	body::Bytes,
	extract::State,
	http::{HeaderMap, StatusCode, Uri, header},
	response::{IntoResponse, Response},
};
use tracing::error;

use crate::state::AppState;

pub async fn forward(State(st): State<AppState>, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
	let path = uri.path().trim_start_matches("/db");
	let url = format!("{}{}", st.db_auth_base, path);

	let mut request = st.http.post(url).body(body.to_vec());
	if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
		request = request.header(header::CONTENT_TYPE, content_type);
	}
	if let Some(authorization) = headers.get(header::AUTHORIZATION) {
		request = request.header(header::AUTHORIZATION, authorization);
	}

	match request.send().await {
		Ok(response) => {
			let status =
				StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
			let content_type = response
				.headers()
				.get(header::CONTENT_TYPE)
				.and_then(|v| v.to_str().ok())
				.unwrap_or("application/json")
				.to_string();
			match response.bytes().await {
				Ok(bytes) => (status, [(header::CONTENT_TYPE, content_type)], bytes).into_response(),
				Err(e) => {
					error!("db forward failed to read response: {e}");
					StatusCode::BAD_GATEWAY.into_response()
				}
			}
		}
		Err(e) => {
			error!("db forward request failed: {e}");
			StatusCode::BAD_GATEWAY.into_response()
		}
	}
}
