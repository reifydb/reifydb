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

use std::ops::Bound;

use reifydb_value::value::row_number::RowNumber;
use serde::{Deserialize, Serialize};

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
	key::flow_node_internal_state::FlowNodeInternalStateKey,
	util::encoding::keycode::encode_u64,
	window::span::WindowSpan,
};

/// One contribution routed to a window accumulator.
pub enum AccumulatorEvent<C> {
	Add(C),
	Remove(C),
}

/// How an engine treats an event whose window coordinate is below the per-group
/// high-water mark (an event for an already-closed window).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LatePolicy {
	/// Event-time semantics: drop late events (deterministic under replay).
	#[default]
	Drop,
	/// Accept late events into their (re-opened) window. High-water still
	/// tracks the max coordinate seen but never rejects.
	Process,
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
	pub row_number: RowNumber,
	pub group: G,
	pub span: WindowSpan<Coord>,
	pub value: Output,
	/// The finalized value before this batch's events, when the window was
	/// non-empty (used by faces that emit a real pre on Update/Remove). `None`
	/// for a brand-new window. Faces that don't need it (the sdk drivers)
	/// ignore it.
	pub prior: Option<Output>,
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

pub fn expiry_key<G>(expiry: u64, group: &G, suffix: &[u8]) -> EncodedKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	let group = group.into_encoded_key();
	let group = group.as_ref();
	let mut bytes = Vec::with_capacity(1 + 8 + group.len() + suffix.len());
	bytes.push(FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG);
	bytes.extend_from_slice(&encode_u64(expiry));
	bytes.extend_from_slice(group);
	bytes.extend_from_slice(suffix);
	EncodedKey::new(bytes)
}

pub fn expiry_due_range(threshold: u64) -> EncodedKeyRange {
	let mut start = Vec::with_capacity(1 + 8);
	start.push(FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG);
	start.extend_from_slice(&encode_u64(threshold));
	let end = vec![FlowNodeInternalStateKey::WINDOW_EXPIRY_TAG + 1];
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), Bound::Excluded(EncodedKey::new(end)))
}
