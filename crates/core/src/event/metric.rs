// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Storage and CDC statistics events.
//!
//! These events are emitted when storage operations occur that need stats tracking.
//! The metrics worker listens to these events and updates storage statistics.

use reifydb_type::value::{datetime::DateTime, duration::Duration};
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion, encoded::key::EncodedKey, fingerprint::RequestFingerprint, metric::StatementMetric,
};

define_event! {
	/// Emitted when storage operations are committed that need stats tracking.
	/// Used for both commit-time ops and async drop worker ops.
	pub struct StorageStatsRecordedEvent {
		pub writes: Vec<StorageWrite>,
		pub deletes: Vec<StorageDelete>,
		pub drops: Vec<StorageDrop>,
		pub version: CommitVersion,
	}
}

/// A storage write operation for stats tracking.
#[derive(Clone, Debug)]
pub struct StorageWrite {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

/// A storage delete operation for stats tracking.
#[derive(Clone, Debug)]
pub struct StorageDelete {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

/// A storage drop operation (MVCC cleanup) for stats tracking.
#[derive(Clone, Debug)]
pub struct StorageDrop {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

define_event! {
	/// Emitted when CDC entries are written that need stats tracking.
	pub struct CdcStatsRecordedEvent {
		pub entries: Vec<CdcEntryStats>,
		pub version: CommitVersion,
	}
}

/// A CDC entry for stats tracking.
#[derive(Clone, Debug)]
pub struct CdcEntryStats {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

/// A CDC entry drop for stats tracking.
#[derive(Clone, Debug)]
pub struct CdcEntryDrop {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

define_event! {
	/// Emitted when CDC entries are dropped that need stats tracking.
	pub struct CdcStatsDroppedEvent {
		pub entries: Vec<CdcEntryDrop>,
		pub version: CommitVersion,
	}
}

/// Detailed telemetry specific to the type of request executed.
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
	/// Emitted when a server request execution is completed.
	pub struct RequestExecutedEvent {
		pub request: Request,
		pub total: Duration,
		pub compute: Duration,
		pub success: bool,
		pub timestamp: DateTime,
	}
}
