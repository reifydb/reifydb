// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::util::cowvec::CowVec;

use crate::{common::CommitVersion, delta::Delta, interface::change::Change};

define_event! {
	pub struct PostCommitEvent {
		pub deltas: CowVec<Delta>,
		pub version: CommitVersion,
		pub flow_changes: Vec<Change>,
	}
}
