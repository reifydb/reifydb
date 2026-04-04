// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Point-in-time snapshot of a [`Counter`](crate::counter::Counter).
#[derive(Debug, Clone)]
pub struct CounterSnapshot {
	pub name: &'static str,
	pub help: &'static str,
	pub value: f64,
}

/// Point-in-time snapshot of a [`Gauge`](crate::gauge::Gauge).
#[derive(Debug, Clone)]
pub struct GaugeSnapshot {
	pub name: &'static str,
	pub help: &'static str,
	pub value: f64,
}

/// Computed percentiles from a histogram distribution.
#[derive(Debug, Clone, Default)]
pub struct Percentiles {
	pub p5: f64,
	pub p10: f64,
	pub p15: f64,
	pub p20: f64,
	pub p25: f64,
	pub p30: f64,
	pub p35: f64,
	pub p40: f64,
	pub p45: f64,
	pub p50: f64,
	pub p55: f64,
	pub p60: f64,
	pub p65: f64,
	pub p70: f64,
	pub p75: f64,
	pub p80: f64,
	pub p85: f64,
	pub p90: f64,
	pub p95: f64,
	pub p96: f64,
	pub p97: f64,
	pub p98: f64,
	pub p99: f64,
	pub max: f64,
}

/// Point-in-time snapshot of a [`Histogram`](crate::histogram::Histogram).
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
	pub name: &'static str,
	pub help: &'static str,
	pub boundaries: &'static [f64],
	pub buckets: Vec<u64>,
	pub sum: f64,
	pub count: u64,
	pub percentiles: Percentiles,
}

/// Unified snapshot over all metric types.
#[derive(Debug, Clone)]
pub enum MetricSnapshot {
	Counter(CounterSnapshot),
	Gauge(GaugeSnapshot),
	Histogram(Box<HistogramSnapshot>),
}
