// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::profiler::ProfilerCategoryId;
use reifydb_metric::gauge::Gauge;

pub struct CategoryGauges {
	pub calls: Arc<Gauge>,
	pub p50: Arc<Gauge>,
	pub p75: Arc<Gauge>,
	pub p90: Arc<Gauge>,
	pub p95: Arc<Gauge>,
	pub p99: Arc<Gauge>,
}

macro_rules! category_gauges {
	($static_name:ident, $cat:literal) => {
		pub static $static_name: LazyLock<CategoryGauges> = LazyLock::new(|| CategoryGauges {
			calls: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".calls"),
				concat!("Total calls observed for category ", $cat),
			)),
			p50: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".p50_us"),
				concat!("Last-snapshot p50 (us) for category ", $cat),
			)),
			p75: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".p75_us"),
				concat!("Last-snapshot p75 (us) for category ", $cat),
			)),
			p90: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".p90_us"),
				concat!("Last-snapshot p90 (us) for category ", $cat),
			)),
			p95: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".p95_us"),
				concat!("Last-snapshot p95 (us) for category ", $cat),
			)),
			p99: Arc::new(Gauge::new(
				concat!("profiler.", $cat, ".p99_us"),
				concat!("Last-snapshot p99 (us) for category ", $cat),
			)),
		});
	};
}

category_gauges!(PROFILER_QUERY_GAUGES, "query");
category_gauges!(PROFILER_TXN_GAUGES, "txn");
category_gauges!(PROFILER_STORAGE_GAUGES, "storage");
category_gauges!(PROFILER_PLAN_GAUGES, "plan");
category_gauges!(PROFILER_CDC_GAUGES, "cdc");
category_gauges!(PROFILER_FLOW_GAUGES, "flow");

pub fn gauges_for(category: ProfilerCategoryId) -> Option<&'static CategoryGauges> {
	match category.0 {
		0 => Some(&PROFILER_QUERY_GAUGES),
		1 => Some(&PROFILER_TXN_GAUGES),
		2 => Some(&PROFILER_STORAGE_GAUGES),
		3 => Some(&PROFILER_PLAN_GAUGES),
		4 => Some(&PROFILER_CDC_GAUGES),
		5 => Some(&PROFILER_FLOW_GAUGES),
		_ => None,
	}
}
