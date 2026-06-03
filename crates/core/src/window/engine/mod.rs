// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Schema-agnostic windowing state-machine engines.
//!
//! Each engine owns the per-(group,window) accumulator state, high-water late
//! rejection, eviction, and diff routing (`Insert -> add`,
//! `Update -> remove(pre) + add(post)`, `Remove -> remove(pre)`). The caller
//! (the "face") owns extraction (`row -> (group, coord, contribution)`) and
//! output construction; it hands the engine pre-bucketed events and receives
//! [`WindowResult`]s to translate into diffs.

pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod tumbling;
pub mod tumbling_carry;

use serde::{Deserialize, Serialize};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	key::flow_node_internal_state::FlowNodeInternalStateKey,
	window::span::WindowSpan,
};

/// One contribution routed to a window accumulator.
pub enum AccEvent<C> {
	Add(C),
	Remove(C),
}

/// How a finalized window value should be emitted downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitKind {
	Insert,
	Update,
	Remove,
}

/// A finalized window the engine produced; the face turns it into a diff.
pub struct WindowResult<G, Coord, Output> {
	pub row_number: reifydb_value::value::row_number::RowNumber,
	pub group: G,
	pub span: WindowSpan<Coord>,
	pub value: Output,
	pub kind: EmitKind,
}

/// Per-group metadata: the highest window start seen, used to drop late events
/// for already-closed windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "K: Serialize", deserialize = "K: serde::de::DeserializeOwned"))]
pub struct GroupMeta<K> {
	pub high_water: Option<K>,
}

impl<K> Default for GroupMeta<K> {
	fn default() -> Self {
		Self {
			high_water: None,
		}
	}
}

/// State-cache key for a group's [`GroupMeta`], tagged so it lives in a
/// distinct keyspace from the per-window accumulators.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct MetaKey(pub EncodedKey);

impl IntoEncodedKey for &MetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		let inner = self.0.as_ref();
		let mut bytes = Vec::with_capacity(1 + inner.len());
		bytes.push(FlowNodeInternalStateKey::WINDOW_META_TAG);
		bytes.extend_from_slice(inner);
		EncodedKey::new(bytes)
	}
}

pub fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}
