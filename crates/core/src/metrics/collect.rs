// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::metrics::sample::MetricsSample;

pub trait MetricsCollector: Send + Sync {
	fn collect(&self, out: &mut Vec<MetricsSample>);
}
