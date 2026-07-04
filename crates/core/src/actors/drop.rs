// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;

use crate::{common::CommitVersion, interface::store::EntryKind};

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

	PersistentEvict {
		table: EntryKind,
		keys: Vec<(EncodedKey, CommitVersion)>,
	},

	Tick,

	Shutdown,
}
