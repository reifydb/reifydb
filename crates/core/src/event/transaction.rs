// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::{common::CommitVersion, delta::Delta};

define_event! {
	pub struct PostCommitEvent {
		pub deltas: CowVec<Delta>,
		pub version: CommitVersion,
	}
}
