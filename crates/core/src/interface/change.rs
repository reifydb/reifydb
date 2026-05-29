// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_abi::flow::diff::DiffType;
use reifydb_value::{Result, value::datetime::DateTime};
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
		post: Columns,
		origin: Option<ChangeOrigin>,
	},
	Update {
		pre: Columns,
		post: Columns,
		origin: Option<ChangeOrigin>,
	},
	Remove {
		pre: Columns,
		origin: Option<ChangeOrigin>,
	},
}

impl Diff {
	pub fn insert(post: Columns) -> Self {
		Self::Insert {
			post,
			origin: None,
		}
	}

	pub fn update(pre: Columns, post: Columns) -> Self {
		Self::Update {
			pre,
			post,
			origin: None,
		}
	}

	pub fn remove(pre: Columns) -> Self {
		Self::Remove {
			pre,
			origin: None,
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
				..
			} => Some(pre),
		}
	}

	pub fn post(&self) -> Option<&Columns> {
		match self {
			Diff::Insert {
				post,
				..
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
				..
			} => post.row_count(),
			Diff::Update {
				post,
				..
			} => post.row_count(),
			Diff::Remove {
				pre,
				..
			} => pre.row_count(),
		}
	}

	pub fn origin(&self) -> Option<&ChangeOrigin> {
		match self {
			Diff::Insert {
				origin,
				..
			} => origin.as_ref(),
			Diff::Update {
				origin,
				..
			} => origin.as_ref(),
			Diff::Remove {
				origin,
				..
			} => origin.as_ref(),
		}
	}

	pub fn set_origin(&mut self, new_origin: Option<ChangeOrigin>) {
		match self {
			Diff::Insert {
				origin,
				..
			} => *origin = new_origin,
			Diff::Update {
				origin,
				..
			} => *origin = new_origin,
			Diff::Remove {
				origin,
				..
			} => *origin = new_origin,
		}
	}

	pub fn effective_origin<'a>(&'a self, parent: &'a ChangeOrigin) -> &'a ChangeOrigin {
		self.origin().unwrap_or(parent)
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
		for mut ch in iter {
			if ch.changed_at > merged.changed_at {
				merged.changed_at = ch.changed_at;
			}
			if ch.origin != merged.origin {
				for diff in ch.diffs.iter_mut() {
					if diff.origin().is_none() {
						diff.set_origin(Some(ch.origin.clone()));
					}
				}
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
		let original = mem::take(&mut self.diffs);
		let mut merged: Diffs = SmallVec::with_capacity(original.len());
		for diff in original {
			if diff.row_count() == 0 {
				continue;
			}
			let same_kind_and_origin = match (merged.last(), &diff) {
				(Some(last), next) => last.kind() == next.kind() && last.origin() == next.origin(),
				_ => false,
			};
			if same_kind_and_origin {
				let last = merged.last_mut().expect("non-empty by same_kind_and_origin branch");
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
	match (target, source) {
		(
			Diff::Insert {
				post: t,
				..
			},
			Diff::Insert {
				post: s,
				..
			},
		) => t.append_all(s),
		(
			Diff::Update {
				pre: tp,
				post: tpost,
				..
			},
			Diff::Update {
				pre: sp,
				post: spost,
				..
			},
		) => {
			tp.append_all(sp)?;
			tpost.append_all(spost)
		}
		(
			Diff::Remove {
				pre: t,
				..
			},
			Diff::Remove {
				pre: s,
				..
			},
		) => t.append_all(s),
		_ => unreachable!("merge_into requires matching diff kinds"),
	}
}
