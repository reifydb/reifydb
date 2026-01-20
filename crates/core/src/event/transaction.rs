// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::{common::CommitVersion, delta::Delta};

define_event! {
	pub struct PostCommitEvent {
		pub deltas: CowVec<Delta>,
		pub version: CommitVersion,
	}
}
