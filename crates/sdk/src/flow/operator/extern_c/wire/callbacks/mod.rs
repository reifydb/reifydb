// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod dictionary;
pub mod state;

use self::{dictionary::DictionaryCallbacks, state::StateCallbacks};
use crate::common::extern_c::wire::callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorCallbacks {
	pub memory: MemoryCallbacks,

	pub state: StateCallbacks,

	pub builder: BuilderCallbacks,

	pub dictionary: DictionaryCallbacks,
}
