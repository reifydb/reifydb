// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::util::cowvec::CowVec;
use reifydb_wal::lsn::Lsn;

use crate::{common::ChangeVersion, delta::Delta};

define_event! {
	pub struct PostCommitEvent {
		pub deltas: CowVec<Delta>,
		pub version: ChangeVersion,
		pub lsn: Lsn,
	}
}
