// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::marker::PhantomData;

use reifydb_core::key::operator_state::GroupId;

pub(crate) struct GroupCache<V> {
	marker: PhantomData<fn() -> V>,
}

impl<V> GroupCache<V> {
	pub(crate) fn new() -> Self {
		Self {
			marker: PhantomData,
		}
	}

	pub(crate) fn get(&self, _group: GroupId) -> Option<V> {
		None
	}

	pub(crate) fn insert(&self, _group: GroupId, _value: V) {}

	pub(crate) fn remove(&self, _group: GroupId) {}
}
