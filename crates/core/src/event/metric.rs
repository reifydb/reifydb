// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage and CDC statistics events.
//!
//! These events are emitted when storage operations occur that need stats tracking.
//! The metrics worker listens to these events and updates storage statistics.

use crate::{common::CommitVersion, impl_event, value::encoded::key::EncodedKey};

/// Emitted when storage operations are committed that need stats tracking.
/// Used for both commit-time ops and async drop worker ops.
#[derive(Clone, Debug)]
pub struct StorageStatsRecordedEvent {
	pub writes: Vec<StorageWrite>,
	pub deletes: Vec<StorageDelete>,
	pub drops: Vec<StorageDrop>,
	pub version: CommitVersion,
}

impl_event!(StorageStatsRecordedEvent);

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

/// Emitted when CDC entries are written that need stats tracking.
#[derive(Clone, Debug)]
pub struct CdcStatsRecordedEvent {
	pub entries: Vec<CdcEntryStats>,
	pub version: CommitVersion,
}

impl_event!(CdcStatsRecordedEvent);

/// A CDC entry for stats tracking.
#[derive(Clone, Debug)]
pub struct CdcEntryStats {
	pub key: EncodedKey,
	pub value_bytes: u64,
}
