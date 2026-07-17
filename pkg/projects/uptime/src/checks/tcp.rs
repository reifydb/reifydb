// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use tokio::net::TcpStream;

use crate::{
	checks::{CheckOutcome, elapsed_ms, resolve_guarded},
	state::AppState,
	store::MonitorRow,
};

pub async fn run(st: &AppState, monitor: &MonitorRow) -> CheckOutcome {
	let Some((host, port)) = monitor.target.rsplit_once(':') else {
		return CheckOutcome::failure("tcp target must be host:port");
	};
	let Ok(port) = port.parse::<u16>() else {
		return CheckOutcome::failure("tcp target has an invalid port");
	};
	let addrs = match resolve_guarded(st, host, port).await {
		Ok(addrs) => addrs,
		Err(e) => return CheckOutcome::failure(e),
	};

	let started = st.clock.instant();
	match TcpStream::connect(addrs.as_slice()).await {
		Ok(_) => CheckOutcome {
			success: true,
			response_time_ms: Some(elapsed_ms(&started)),
			status_code: None,
			error: None,
		},
		Err(e) => CheckOutcome {
			success: false,
			response_time_ms: Some(elapsed_ms(&started)),
			status_code: None,
			error: Some(format!("connect failed: {e}")),
		},
	}
}
