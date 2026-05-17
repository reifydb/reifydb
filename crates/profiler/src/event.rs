// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! EventBus payloads dispatched by the profiler on scope close and on mid-scope batch flush. Carried as `Arc`-wrapped
//! `ProfileSummary` so listeners observe cheap clones. Lives in the primitive crate so both producer (sub-profiler's
//! sink) and consumer (sub-profiler's listener) can share the type without a metric or IoC dependency.

use std::sync::Arc;

#[allow(unused_imports)]
use paste as _;
use reifydb_core::define_event;

use crate::summary::ProfileSummary;

define_event! {

	pub struct ProfileScopeClosedEvent {
		pub summary: Arc<ProfileSummary>,
	}
}

define_event! {

	pub struct ProfileScopeBatchEvent {
		pub summary: Arc<ProfileSummary>,
	}
}
