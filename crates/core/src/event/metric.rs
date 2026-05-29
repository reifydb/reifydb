// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion, encoded::key::EncodedKey, fingerprint::RequestFingerprint, metric::StatementMetric,
	profiler::ProfilerCategoryId,
};

define_event! {


	pub struct MultiCommittedEvent {
		pub writes: Vec<MultiWrite>,
		pub deletes: Vec<MultiDelete>,
		pub drops: Vec<MultiDrop>,
		pub version: CommitVersion,
	}
}

#[derive(Clone, Debug)]
pub struct MultiWrite {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct MultiDelete {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct MultiDrop {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

define_event! {

	pub struct CdcWrittenEvent {
		pub entries: Vec<CdcWrite>,
		pub version: CommitVersion,
	}
}

#[derive(Clone, Debug)]
pub struct CdcWrite {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct CdcEviction {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

define_event! {

	pub struct CdcEvictedEvent {
		pub entries: Vec<CdcEviction>,
		pub version: CommitVersion,
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
	Query {
		fingerprint: RequestFingerprint,
		statements: Vec<StatementMetric>,
	},
	Command {
		fingerprint: RequestFingerprint,
		statements: Vec<StatementMetric>,
	},
	Admin {
		fingerprint: RequestFingerprint,
		statements: Vec<StatementMetric>,
	},
}

define_event! {

	pub struct RequestExecutedEvent {
		pub request: Request,
		pub total: Duration,
		pub compute: Duration,
		pub success: bool,
		pub timestamp: DateTime,
	}
}

#[derive(Clone, Debug)]
pub struct ProfilerAggregateRow {
	pub category: ProfilerCategoryId,
	pub span_name: String,
	pub dim_1: Option<String>,
	pub dim_2: Option<String>,
	pub calls: u64,
	pub total_us: u64,
	pub min_us: u32,
	pub max_us: u32,
	pub p50_us: u32,
	pub p60_us: u32,
	pub p70_us: u32,
	pub p75_us: u32,
	pub p80_us: u32,
	pub p85_us: u32,
	pub p90_us: u32,
	pub p95_us: u32,
	pub p98_us: u32,
	pub p99_us: u32,
	pub extras_sum: [u64; 4],
}

define_event! {

	pub struct ProfilerSnapshotEvent {
		pub timestamp: DateTime,
		pub rows: Vec<ProfilerAggregateRow>,
	}
}
