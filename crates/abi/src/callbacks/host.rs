// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::{
	builder::BuilderCallbacks, dictionary::DictionaryCallbacks, memory::MemoryCallbacks, rql::RqlCallbacks,
	state::StateCallbacks,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostCallbacks {
	pub memory: MemoryCallbacks,
	pub state: StateCallbacks,
	pub rql: RqlCallbacks,
	pub builder: BuilderCallbacks,
	pub dictionary: DictionaryCallbacks,
}
