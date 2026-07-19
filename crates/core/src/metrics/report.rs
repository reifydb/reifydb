// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::metrics::sample::MetricsSample;

pub trait MetricsReporter: Send + Sync {
	fn read(&self, out: &mut Vec<MetricsSample>);
}
