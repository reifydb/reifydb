// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod dns;
mod http;
mod ping;
mod tcp;

use std::net::{IpAddr, SocketAddr};

use reifydb::runtime::context::clock::Instant;
use tokio::{net::lookup_host, time::timeout as tokio_timeout};

use crate::{state::AppState, store::MonitorRow};

pub struct CheckOutcome {
	pub success: bool,
	pub response_time_ms: Option<i64>,
	pub status_code: Option<i16>,
	pub error: Option<String>,
}

impl CheckOutcome {
	pub fn failure(error: impl Into<String>) -> Self {
		Self {
			success: false,
			response_time_ms: None,
			status_code: None,
			error: Some(error.into()),
		}
	}
}

pub async fn run_check(st: &AppState, monitor: &MonitorRow) -> CheckOutcome {
	let timeout = monitor.timeout.to_std();
	let run = async {
		match monitor.kind.as_str() {
			"http" => self::http::run(st, monitor).await,
			"tcp" => tcp::run(st, monitor).await,
			"ping" => ping::run(st, monitor).await,
			"dns" => dns::run(st, monitor).await,
			other => CheckOutcome::failure(format!("unknown check kind: {other}")),
		}
	};
	match tokio_timeout(timeout, run).await {
		Ok(outcome) => outcome,
		Err(_) => CheckOutcome::failure(format!(
			"check timed out after {} ms",
			monitor.timeout.milliseconds().unwrap_or(0)
		)),
	}
}

fn ip_is_public(ip: &IpAddr) -> bool {
	match ip {
		IpAddr::V4(v4) => {
			let octets = v4.octets();
			let cgnat = octets[0] == 100 && (64..128).contains(&octets[1]);
			!(v4.is_loopback()
				|| v4.is_private() || v4.is_link_local()
				|| v4.is_broadcast() || v4.is_unspecified()
				|| cgnat)
		}
		IpAddr::V6(v6) => {
			let unique_local = (v6.segments()[0] & 0xfe00) == 0xfc00;
			let link_local = (v6.segments()[0] & 0xffc0) == 0xfe80;
			!(v6.is_loopback() || v6.is_unspecified() || unique_local || link_local)
		}
	}
}

pub async fn resolve_guarded(st: &AppState, host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
	let addrs: Vec<SocketAddr> =
		lookup_host((host, port)).await.map_err(|e| format!("dns resolution failed: {e}"))?.collect();
	if addrs.is_empty() {
		return Err("dns resolution returned no addresses".to_string());
	}
	if !st.cfg.allow_private_targets
		&& let Some(private) = addrs.iter().find(|a| !ip_is_public(&a.ip()))
	{
		return Err(format!(
			"target resolves to a non-public address ({}); start with --allow-private-targets to permit this",
			private.ip()
		));
	}
	Ok(addrs)
}

pub fn elapsed_ms(started: &Instant) -> i64 {
	i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
	use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

	use super::ip_is_public;

	#[test]
	fn private_and_special_ranges_are_not_public() {
		// SSRF guard: every one of these must be rejected for hosted
		// deployments, where checks run from inside the infrastructure.
		let blocked = [
			IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
			IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
			IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
			IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
			IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
			IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
			IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
			IpAddr::V6(Ipv6Addr::LOCALHOST),
			IpAddr::V6("fc00::1".parse().unwrap()),
			IpAddr::V6("fe80::1".parse().unwrap()),
		];
		for ip in blocked {
			assert!(!ip_is_public(&ip), "{ip} must not be considered public");
		}
	}

	#[test]
	fn public_addresses_are_allowed() {
		let allowed = [
			IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
			IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
			IpAddr::V6("2606:4700:4700::1111".parse().unwrap()),
		];
		for ip in allowed {
			assert!(ip_is_public(&ip), "{ip} must be considered public");
		}
	}
}
