// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::fingerprint::{RequestFingerprint, StatementFingerprint};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
	pub fingerprint: RequestFingerprint,
	pub statements: Vec<StatementMetric>,
	pub total: Duration,
	pub compute: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementMetric {
	pub fingerprint: StatementFingerprint,
	pub normalized_rql: String,
	pub compile_duration_us: u64,
	pub execute_duration_us: u64,
	pub rows_affected: u64,
}
