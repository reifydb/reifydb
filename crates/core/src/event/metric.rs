// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage and CDC statistics events.
//!
//! These events are emitted when storage operations occur that need stats tracking.
//! The metrics worker listens to these events and updates storage statistics.

use crate::{common::CommitVersion, encoded::key::EncodedKey};

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
