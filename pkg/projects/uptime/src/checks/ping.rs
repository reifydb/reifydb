// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::net::IpAddr;

use surge_ping::{Client, Config, ICMP, PingIdentifier, PingSequence};

use crate::{
	checks::{CheckOutcome, resolve_guarded},
	state::AppState,
	store::MonitorRow,
};

pub async fn run(st: &AppState, monitor: &MonitorRow) -> CheckOutcome {
	let addrs = match resolve_guarded(st, &monitor.target, 0).await {
		Ok(addrs) => addrs,
		Err(e) => return CheckOutcome::failure(e),
	};
	let ip = addrs[0].ip();

	let config = match ip {
		IpAddr::V4(_) => Config::default(),
		IpAddr::V6(_) => Config::builder().kind(ICMP::V6).build(),
	};
	let client = match Client::new(&config) {
		Ok(client) => client,
		Err(e) => {
			return CheckOutcome::failure(format!("icmp unavailable on this host ({e}); \
				 unprivileged ping requires net.ipv4.ping_group_range to include this process"));
		}
	};

	let ident = u16::from_be_bytes([st.rng.infra_bytes_32()[0], st.rng.infra_bytes_32()[1]]);
	let mut pinger = client.pinger(ip, PingIdentifier(ident)).await;
	#[allow(clippy::disallowed_types)]
	pinger.timeout(monitor.timeout.to_std());

	match pinger.ping(PingSequence(0), &[0u8; 32]).await {
		Ok((_, rtt)) => CheckOutcome {
			success: true,
			response_time_ms: Some(i64::try_from(rtt.as_millis()).unwrap_or(i64::MAX)),
			status_code: None,
			error: None,
		},
		Err(e) => CheckOutcome {
			success: false,
			response_time_ms: None,
			status_code: None,
			error: Some(format!("ping failed: {e}")),
		},
	}
}
