// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_value::util::cowvec::CowVec;

use crate::common::CommitVersion;

define_event! {


	pub struct MetricsProcessedEvent {
		pub up_to: CommitVersion,
	}
}

define_event! {
	pub struct MultiVersionPersistedEvent {
		pub version: CommitVersion,
		pub keys: CowVec<EncodedKey>,
	}
}
