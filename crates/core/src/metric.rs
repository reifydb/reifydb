// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::fingerprint::{RequestFingerprint, StatementFingerprint};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
	pub request_fingerprint: RequestFingerprint,
	pub statements: Vec<StatementMetric>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementMetric {
	pub fingerprint: StatementFingerprint,
	pub normalized_rql: String,
	pub compile_duration_us: u64,
	pub execute_duration_us: u64,
	pub rows_affected: u64,
}
