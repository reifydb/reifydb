// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{datetime::DateTime, duration::Duration};
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion, encoded::key::EncodedKey, fingerprint::RequestFingerprint, metric::StatementMetric,
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
