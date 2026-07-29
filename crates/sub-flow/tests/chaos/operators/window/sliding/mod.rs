// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod regression;

use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::driver,
	operators::window::{
		WindowSpec, build,
		grid::{Grid, GridOracle},
	},
};

pub struct Params {
	pub size_secs: u64,
	pub slide_secs: u64,
	pub grace_secs: u64,
	pub groups: i32,
	pub steps: u32,
	pub max_batch: u32,
	pub coord_span_ms: u64,
	pub remove_pct: u32,
	pub update_pct: u32,
	pub seal_pct: u32,
}

struct SlidingGrid {
	size_ms: u64,
	slide_ms: u64,
}

impl Grid for SlidingGrid {
	fn windows_of(&self, coord_ms: u64) -> Vec<u64> {
		// Windows start on multiples of the slide, so the candidates are bounded by the first
		// slide that could still reach coord_ms and the last one that has started by it. The
		// containment filter is the authority - the bounds are only there to keep the range
		// finite, deliberately loose so a wrong bound cannot silently drop a window.
		let lowest = coord_ms.saturating_sub(self.size_ms.saturating_sub(1)) / self.slide_ms;
		let highest = coord_ms / self.slide_ms;
		(lowest..=highest)
			.map(|wid| wid * self.slide_ms)
			.filter(|start| coord_ms >= *start && coord_ms < start + self.size_ms)
			.collect()
	}
}

pub fn drive(seed: u64, params: Params) {
	let size_ms = params.size_secs * 1_000;
	let slide_ms = params.slide_secs * 1_000;
	let grace_ms = params.grace_secs * 1_000;
	assert!(slide_ms < size_ms, "the sweep only covers overlapping sliding windows; slide must be < size");

	let spec = WindowSpec {
		kind: WindowKind::Sliding {
			size: WindowSize::Duration(Duration::from_seconds(params.size_secs as i64).unwrap()),
			slide: WindowSize::Duration(Duration::from_seconds(params.slide_secs as i64).unwrap()),
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
		GridOracle::new(
			SlidingGrid {
				size_ms,
				slide_ms,
			},
			size_ms,
			grace_ms,
		),
	);
}
