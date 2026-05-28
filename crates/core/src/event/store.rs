// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::util::cowvec::CowVec;

use crate::{common::CommitVersion, encoded::key::EncodedKey};

define_event! {


	pub struct StatsProcessedEvent {
		pub up_to: CommitVersion,
	}
}

define_event! {
	pub struct MultiVersionPersistedEvent {
		pub version: CommitVersion,
		pub keys: CowVec<EncodedKey>,
	}
}
