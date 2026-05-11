// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::flow::diff::DiffType;
use reifydb_type::{Result, value::datetime::DateTime};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	value::column::columns::Columns,
};

pub type Diffs = SmallVec<[Diff; 4]>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

	pub fn row_count(&self) -> usize {
		match self {
			Diff::Insert {
				post,
			} => post.row_count(),
			Diff::Update {
				post,
				..
			} => post.row_count(),
			Diff::Remove {
				pre,
			} => pre.row_count(),
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

	pub fn row_count(&self) -> usize {
		self.diffs.iter().map(Diff::row_count).sum()
	}

	pub fn merge(changes: Vec<Change>) -> Result<Change> {
		let mut iter = changes.into_iter();
		let mut merged = iter.next().expect("Change::merge requires at least one Change");
		for ch in iter {
			if ch.changed_at > merged.changed_at {
				merged.changed_at = ch.changed_at;
			}
			merged.diffs.extend(ch.diffs);
		}
		merged.coalesce()?;
		Ok(merged)
	}

	pub fn coalesce(&mut self) -> Result<()> {
		if self.diffs.len() <= 1 {
			return Ok(());
		}
		let original = std::mem::take(&mut self.diffs);
		let mut merged: Diffs = SmallVec::with_capacity(original.len());
		for diff in original {
			if diff.row_count() == 0 {
				continue;
			}
			let same_kind = match (merged.last(), &diff) {
				(
					Some(Diff::Insert {
						..
					}),
					Diff::Insert {
						..
					},
				) => true,
				(
					Some(Diff::Update {
						..
					}),
					Diff::Update {
						..
					},
				) => true,
				(
					Some(Diff::Remove {
						..
					}),
					Diff::Remove {
						..
					},
				) => true,
				_ => false,
			};
			if same_kind {
				let last = merged.last_mut().expect("non-empty by same_kind branch");
				merge_into(last, diff)?;
			} else {
				merged.push(diff);
			}
		}
		self.diffs = merged;
		Ok(())
	}
}

fn merge_into(target: &mut Diff, source: Diff) -> Result<()> {
	fn unwrap_or_clone(arc: Arc<Columns>) -> Columns {
		Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone())
	}
	match (target, source) {
		(
			Diff::Insert {
				post: t,
			},
			Diff::Insert {
				post: s,
			},
		) => Arc::make_mut(t).append_all(unwrap_or_clone(s)),
		(
			Diff::Update {
				pre: tp,
				post: tpost,
			},
			Diff::Update {
				pre: sp,
				post: spost,
			},
		) => {
			Arc::make_mut(tp).append_all(unwrap_or_clone(sp))?;
			Arc::make_mut(tpost).append_all(unwrap_or_clone(spost))
		}
		(
			Diff::Remove {
				pre: t,
			},
			Diff::Remove {
				pre: s,
			},
		) => Arc::make_mut(t).append_all(unwrap_or_clone(s)),
		_ => unreachable!("merge_into requires matching diff kinds"),
	}
}
