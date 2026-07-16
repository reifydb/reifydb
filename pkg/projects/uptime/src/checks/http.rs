// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	checks::{CheckOutcome, elapsed_ms, resolve_guarded},
	state::AppState,
	store::MonitorRow,
};

const MAX_BODY_BYTES: usize = 512 * 1024;

pub async fn run(st: &AppState, monitor: &MonitorRow) -> CheckOutcome {
	let url = match reqwest::Url::parse(&monitor.target) {
		Ok(url) => url,
		Err(e) => return CheckOutcome::failure(format!("invalid url: {e}")),
	};
	let Some(host) = url.host_str() else {
		return CheckOutcome::failure("url has no host");
	};
	let port = url.port_or_known_default().unwrap_or(80);
	if let Err(e) = resolve_guarded(st, host, port).await {
		return CheckOutcome::failure(e);
	}

	let method = match monitor.http_method.as_deref() {
		Some("HEAD") => reqwest::Method::HEAD,
		_ => reqwest::Method::GET,
	};

	let started = st.clock.instant();
	#[allow(clippy::disallowed_types)]
	let request = st.http.request(method, url).timeout(monitor.timeout.to_std());
	let response = match request.send().await {
		Ok(response) => response,
		Err(e) => {
			return CheckOutcome {
				success: false,
				response_time_ms: Some(elapsed_ms(&started)),
				status_code: None,
				error: Some(format!("request failed: {e}")),
			};
		}
	};

	let status = response.status();
	let status_code = i16::try_from(status.as_u16()).ok();

	let status_ok = match monitor.expected_status {
		Some(expected) => i32::from(status.as_u16()) == i32::from(expected),
		None => status.is_success(),
	};

	let keyword_error = match (&monitor.keyword, status_ok) {
		(Some(keyword), true) => match read_body_capped(response).await {
			Ok(body) => {
				if body.contains(keyword) {
					None
				} else {
					Some(format!("keyword \"{keyword}\" not found in response body"))
				}
			}
			Err(e) => Some(format!("failed to read response body: {e}")),
		},
		_ => None,
	};

	let response_time_ms = Some(elapsed_ms(&started));

	if !status_ok {
		let expectation = match monitor.expected_status {
			Some(expected) => format!("expected status {expected}"),
			None => "expected a 2xx status".to_string(),
		};
		return CheckOutcome {
			success: false,
			response_time_ms,
			status_code,
			error: Some(format!("got status {}, {expectation}", status.as_u16())),
		};
	}

	match keyword_error {
		Some(error) => CheckOutcome {
			success: false,
			response_time_ms,
			status_code,
			error: Some(error),
		},
		None => CheckOutcome {
			success: true,
			response_time_ms,
			status_code,
			error: None,
		},
	}
}

async fn read_body_capped(mut response: reqwest::Response) -> Result<String, reqwest::Error> {
	let mut body: Vec<u8> = Vec::new();
	while let Some(chunk) = response.chunk().await? {
		let remaining = MAX_BODY_BYTES.saturating_sub(body.len());
		body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
		if body.len() >= MAX_BODY_BYTES {
			break;
		}
	}
	Ok(String::from_utf8_lossy(&body).into_owned())
}
