// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod regression;

use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::driver,
	operators::window::{WindowSpec, build, rolling::oracle::Oracle},
};

pub struct Params {
	pub size_secs: u64,
	pub grace_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

pub fn drive(seed: u64, params: Params) {
	let size_ms = params.size_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;

	let spec = WindowSpec {
		kind: WindowKind::Rolling {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			lag: None,
		},
		domain: TimeDomain::Event,
		group_by: "g",
		aggregations: "total: math::sum(v)",
		grace: Duration::from_seconds(params.grace_secs as i64).unwrap(),
		lateness: Duration::default(),
	};

	driver::drive(
		seed,
		driver::Params {
			groups: params.groups,
			steps: params.steps,
			max_batch: params.max_batch,
			coord_span_ms: params.coord_span_ms,
			remove_pct: params.remove_pct,
			update_pct: params.update_pct,
			seal_pct: params.seal_pct,
			drain_at_ms: params.coord_span_ms + size_ms + grace_ms + 10_000,
		},
		|runtime| build(&spec, runtime),
		Oracle::new(size_ms, grace_ms),
	);
}
