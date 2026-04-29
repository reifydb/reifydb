// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{
	builder::BuilderCallbacks, catalog::CatalogCallbacks, log::LogCallbacks, memory::MemoryCallbacks,
	rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
};

/// Host-provided callbacks for FFI operators
///
/// These callbacks allow operators to request services from the host system
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostCallbacks {
	pub memory: MemoryCallbacks,
	pub state: StateCallbacks,
	pub log: LogCallbacks,
	pub store: StoreCallbacks,
	pub catalog: CatalogCallbacks,
	pub rql: RqlCallbacks,
	pub builder: BuilderCallbacks,
}
