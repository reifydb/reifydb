// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_value::{Result, value::datetime::DateTime};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		consolidate::coalesce_diffs,
	},
	value::column::columns::Columns,
};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffType {
	Insert = 1,

	Update = 2,

	Remove = 3,
}

pub type Diffs = SmallVec<[Diff; 4]>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChangeOrigin {
	Object(ObjectId),
	Flow(OperatorId),
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

	pub fn columns_mut(&mut self) -> impl Iterator<Item = &mut Columns> {
		let pair: [Option<&mut Columns>; 2] = match self {
			Diff::Insert {
				post,
				..
			} => [Some(post), None],
			Diff::Update {
				pre,
				post,
				..
			} => [Some(pre), Some(post)],
			Diff::Remove {
				pre,
				..
			} => [Some(pre), None],
		};
		pair.into_iter().flatten()
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
	pub fn from_object(
		object: ObjectId,
		version: CommitVersion,
		diffs: impl Into<Diffs>,
		changed_at: DateTime,
	) -> Self {
		Self {
			origin: ChangeOrigin::Object(object),
			diffs: diffs.into(),
			version,
			changed_at,
		}
	}

	pub fn from_flow(
		from: OperatorId,
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
		self.diffs = SmallVec::from_vec(coalesce_diffs(original.into_vec())?);
		Ok(())
	}
}
