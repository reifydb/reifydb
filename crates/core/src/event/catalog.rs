// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{common::CommitVersion, define_event};

define_event! {
	pub struct CatalogChangedEvent {
		pub version: CommitVersion,
	}
}
