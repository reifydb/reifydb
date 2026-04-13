// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::{common::CommitVersion, interface::store::EntryKind};

/// A request to drop old versions of a key.
#[derive(Debug, Clone)]
pub struct DropRequest {
	/// The table containing the key.
	pub table: EntryKind,
	/// The logical key (without version suffix).
	pub key: CowVec<u8>,
	/// The commit version that created this drop request.
	pub commit_version: CommitVersion,
	/// A version being written in the same batch (to avoid race).
	pub pending_version: Option<CommitVersion>,
}

/// Messages for the drop actor.
#[derive(Clone)]
pub enum DropMessage {
	/// A single drop request to process.
	Request(DropRequest),
	/// A batch of drop requests to process.
	Batch(Vec<DropRequest>),
	/// Periodic tick for flushing batches.
	Tick,
	/// Shutdown the actor.
	Shutdown,
}
