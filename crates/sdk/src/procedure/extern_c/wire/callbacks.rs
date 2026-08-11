// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::extern_c::wire::callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks, rql::RqlCallbacks};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProcedureCallbacks {
	pub memory: MemoryCallbacks,

	pub rql: RqlCallbacks,

	pub builder: BuilderCallbacks,
}
