// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
