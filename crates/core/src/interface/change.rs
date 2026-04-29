// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::value::datetime::DateTime;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	value::column::columns::Columns,
};

/// Inline-storage container for `Change.diffs`. Most operator emissions
/// produce 1-3 diffs per call; reserving 4 inline avoids the heap allocation
/// in the typical case while spilling to the heap for fan-out-heavy ops.
pub type Diffs = SmallVec<[Diff; 4]>;

/// Origin of a change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeOrigin {
	Shape(ShapeId),
	Flow(FlowNodeId),
}

/// Represents a single diff.
///
/// Carries `Arc<Columns>` so that cloning a `Diff` (or the enclosing
/// `Change`) is a refcount bump rather than a deep copy of every column,
/// and so that producers (e.g. `CdcProducerActor`) can hold onto a slab
/// pool of `Arc<Columns>` and reuse them across calls when `strong_count`
/// drops back to 1 after dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Diff {
	Insert {
		post: Arc<Columns>,
	},
	Update {
		pre: Arc<Columns>,
		post: Arc<Columns>,
	},
	Remove {
		pre: Arc<Columns>,
	},
}

impl Diff {
	/// Build an insert diff from an owned `Columns`. Wraps internally.
	pub fn insert(post: Columns) -> Self {
		Self::Insert {
			post: Arc::new(post),
		}
	}

	/// Build an update diff from owned `Columns`. Wraps internally.
	pub fn update(pre: Columns, post: Columns) -> Self {
		Self::Update {
			pre: Arc::new(pre),
			post: Arc::new(post),
		}
	}

	/// Build a remove diff from an owned `Columns`. Wraps internally.
	pub fn remove(pre: Columns) -> Self {
		Self::Remove {
			pre: Arc::new(pre),
		}
	}

	/// Build an insert diff from an already-`Arc`'d `Columns`. Used by
	/// the `CdcProducerActor` slab pool to avoid an extra `Arc::new`.
	pub fn insert_arc(post: Arc<Columns>) -> Self {
		Self::Insert {
			post,
		}
	}

	/// Build an update diff from already-`Arc`'d `Columns`.
	pub fn update_arc(pre: Arc<Columns>, post: Arc<Columns>) -> Self {
		Self::Update {
			pre,
			post,
		}
	}

	/// Build a remove diff from an already-`Arc`'d `Columns`.
	pub fn remove_arc(pre: Arc<Columns>) -> Self {
		Self::Remove {
			pre,
		}
	}
}

/// A change with origin, diffs, version, and timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
	/// Origin of this change
	pub origin: ChangeOrigin,
	/// The list of diffs (changes)
	pub diffs: Diffs,
	/// Version of this change.
	pub version: CommitVersion,
	/// Timestamp when this was changed
	pub changed_at: DateTime,
}

impl Change {
	/// Create a change from a shape (external) source
	pub fn from_shape(
		shape: ShapeId,
		version: CommitVersion,
		diffs: impl Into<Diffs>,
		changed_at: DateTime,
	) -> Self {
		Self {
			origin: ChangeOrigin::Shape(shape),
			diffs: diffs.into(),
			version,
			changed_at,
		}
	}

	/// Create a change from a flow node (internal)
	pub fn from_flow(
		from: FlowNodeId,
		version: CommitVersion,
		diffs: impl Into<Diffs>,
		changed_at: DateTime,
	) -> Self {
		Self {
			origin: ChangeOrigin::Flow(from),
			diffs: diffs.into(),
			version,
			changed_at,
		}
	}
}
