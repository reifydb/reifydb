// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::flow::diff::DiffType;
use reifydb_type::value::datetime::DateTime;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	value::column::columns::Columns,
};

pub type Diffs = SmallVec<[Diff; 4]>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeOrigin {
	Shape(ShapeId),
	Flow(FlowNodeId),
}

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
	pub fn insert(post: Columns) -> Self {
		Self::Insert {
			post: Arc::new(post),
		}
	}

	pub fn update(pre: Columns, post: Columns) -> Self {
		Self::Update {
			pre: Arc::new(pre),
			post: Arc::new(post),
		}
	}

	pub fn remove(pre: Columns) -> Self {
		Self::Remove {
			pre: Arc::new(pre),
		}
	}

	pub fn insert_arc(post: Arc<Columns>) -> Self {
		Self::Insert {
			post,
		}
	}

	pub fn update_arc(pre: Arc<Columns>, post: Arc<Columns>) -> Self {
		Self::Update {
			pre,
			post,
		}
	}

	pub fn remove_arc(pre: Arc<Columns>) -> Self {
		Self::Remove {
			pre,
		}
	}

	pub fn pre(&self) -> Option<&Columns> {
		match self {
			Diff::Insert {
				..
			} => None,
			Diff::Update {
				pre,
				..
			} => Some(pre),
			Diff::Remove {
				pre,
			} => Some(pre),
		}
	}

	pub fn post(&self) -> Option<&Columns> {
		match self {
			Diff::Insert {
				post,
			} => Some(post),
			Diff::Update {
				post,
				..
			} => Some(post),
			Diff::Remove {
				..
			} => None,
		}
	}

	pub fn kind(&self) -> DiffType {
		match self {
			Diff::Insert {
				..
			} => DiffType::Insert,
			Diff::Update {
				..
			} => DiffType::Update,
			Diff::Remove {
				..
			} => DiffType::Remove,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
	pub origin: ChangeOrigin,

	pub diffs: Diffs,

	pub version: CommitVersion,

	pub changed_at: DateTime,
}

impl Change {
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
