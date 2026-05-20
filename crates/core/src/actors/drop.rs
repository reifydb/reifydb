// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};

#[derive(Debug, Clone)]
pub struct DropRequest {
	pub table: EntryKind,

	pub key: EncodedKey,

	pub commit_version: CommitVersion,

	pub pending_version: Option<CommitVersion>,
}

#[derive(Clone)]
pub enum DropMessage {
	Request(DropRequest),

	Batch(Vec<DropRequest>),

	Tick,

	Shutdown,
}
