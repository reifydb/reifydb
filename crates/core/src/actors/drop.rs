// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::{common::CommitVersion, interface::store::EntryKind};

#[derive(Debug, Clone)]
pub struct DropRequest {
	pub table: EntryKind,

	pub key: CowVec<u8>,

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
