// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	checks::{CheckOutcome, elapsed_ms},
	state::AppState,
	store::MonitorRow,
};

pub async fn run(st: &AppState, monitor: &MonitorRow) -> CheckOutcome {
	let started = st.clock.instant();
	let resolved = tokio::net::lookup_host((monitor.target.as_str(), 0)).await;
	let response_time_ms = Some(elapsed_ms(&started));

	let addrs: Vec<std::net::IpAddr> = match resolved {
		Ok(addrs) => addrs.map(|a| a.ip()).collect(),
		Err(e) => {
			return CheckOutcome {
				success: false,
				response_time_ms,
				status_code: None,
				error: Some(format!("resolution failed: {e}")),
			};
		}
	};

	if addrs.is_empty() {
		return CheckOutcome {
			success: false,
			response_time_ms,
			status_code: None,
			error: Some("resolution returned no addresses".to_string()),
		};
	}

	if let Some(expected) = &monitor.expected_ip {
		let matched = addrs.iter().any(|ip| ip.to_string() == *expected);
		if !matched {
			return CheckOutcome {
				success: false,
				response_time_ms,
				status_code: None,
				error: Some(format!(
					"expected {expected}, resolved to {}",
					addrs.iter().map(|ip| ip.to_string()).collect::<Vec<_>>().join(", ")
				)),
			};
		}
	}

	CheckOutcome {
		success: true,
		response_time_ms,
		status_code: None,
		error: None,
	}
}
