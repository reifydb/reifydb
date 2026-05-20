// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

#[allow(unused_imports)]
use paste as _;
use reifydb_core::define_event;

use crate::summary::ProfilerSummary;

define_event! {

	pub struct ProfilerScopeClosedEvent {
		pub summary: Arc<ProfilerSummary>,
	}
}

define_event! {

	pub struct ProfilerScopeBatchEvent {
		pub summary: Arc<ProfilerSummary>,
	}
}
