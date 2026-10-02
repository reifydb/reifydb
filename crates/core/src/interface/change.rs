// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_array::RecordBatch;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, diff_type::DiffType},
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
	common::ChangeVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		consolidate::coalesce_diffs,
	},
};

pub type Diffs = SmallVec<[Diff; 4]>;

pub type StagedBatch = (DiffType, RecordBatch);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChangeOrigin {
	Object(ObjectId),
	Flow(OperatorId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Diff {
	Insert {
		post: RecordBatch,
		origin: Option<ChangeOrigin>,
	},
	Update {
		pre: RecordBatch,
		post: RecordBatch,
		origin: Option<ChangeOrigin>,
	},
	Remove {
		pre: RecordBatch,
		origin: Option<ChangeOrigin>,
	},
}

impl Diff {
	pub fn insert(post: RecordBatch) -> Self {
		Self::Insert {
			post,
			origin: None,
		}
	}

	pub fn update(pre: RecordBatch, post: RecordBatch) -> Self {
		Self::update_with_origin(pre, post, None)
	}

	pub fn update_with_origin(pre: RecordBatch, post: RecordBatch, origin: Option<ChangeOrigin>) -> Self {
		assert!(
			pre.num_rows() == post.num_rows(),
			"an update change needs as many pre rows as post rows, got {} pre and {} post",
			pre.num_rows(),
			post.num_rows()
		);
		Self::Update {
			pre,
			post,
			origin,
		}
	}

	pub fn remove(pre: RecordBatch) -> Self {
		Self::Remove {
			pre,
			origin: None,
		}
	}

	pub fn pre(&self) -> Option<&RecordBatch> {
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

	pub fn post(&self) -> Option<&RecordBatch> {
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

	pub fn batches_mut(&mut self) -> impl Iterator<Item = &mut RecordBatch> {
		let pair: [Option<&mut RecordBatch>; 2] = match self {
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
			} => post.num_rows(),
			Diff::Update {
				post,
				..
			} => post.num_rows(),
			Diff::Remove {
				pre,
				..
			} => pre.num_rows(),
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
}

#[derive(Debug, Clone)]
pub struct Change {
	pub origin: ChangeOrigin,

	pub diffs: Diffs,

	pub version: ChangeVersion,

	pub changed_at: DateTime,
}

impl Change {
	pub fn from_object(
		object: ObjectId,
		version: ChangeVersion,
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
		version: ChangeVersion,
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
			if ch.version.commit > merged.version.commit {
				merged.version.commit = ch.version.commit;
			}
			if ch.version.source > merged.version.source {
				merged.version.source = ch.version.source;
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
