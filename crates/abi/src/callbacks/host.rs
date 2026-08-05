// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::{
	builder::BuilderCallbacks, dictionary::DictionaryCallbacks, log::LogCallbacks, memory::MemoryCallbacks,
	row_shape::RowShapeCallbacks, rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostCallbacks {
	pub memory: MemoryCallbacks,
	pub state: StateCallbacks,
	pub log: LogCallbacks,
	pub store: StoreCallbacks,
	pub row_shape: RowShapeCallbacks,
	pub rql: RqlCallbacks,
	pub builder: BuilderCallbacks,
	pub dictionary: DictionaryCallbacks,
}
