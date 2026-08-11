// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr, slice::from_raw_parts};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::OperatorId,
			id::{QueueId, RingBufferId, SeriesId, TableId, ViewId},
			object::ObjectId,
			vtable::VTableId,
		},
		change::{Change, ChangeOrigin, Diff, DiffType, Diffs},
	},
};
use reifydb_value::value::{datetime::DateTime, dictionary::DictionaryId};
use tracing::instrument;

use crate::{
	common::extern_c::wire::columns::ExternCColumns,
	flow::{
		extern_c::wire::change::{ExternCChange, ExternCDiff, ExternCOrigin},
		operator::extern_c::binding::arena::Arena,
	},
};

impl Arena {
	#[instrument(name = "flow::marshal::change", level = "trace", skip_all, fields(diff_count = change.diffs.len()))]
	pub fn marshal_change(&mut self, change: &Change) -> ExternCChange {
		let diffs_count = change.diffs.len();
		let diffs_ptr = if diffs_count > 0 {
			let diffs_array = self.alloc(diffs_count * size_of::<ExternCDiff>()) as *mut ExternCDiff;

			// SAFETY: `diffs_count > 0` makes the arena block non-null, and it reserved
			// `diffs_count * size_of::<ExternCDiff>()` bytes at alignment 8, so every `add(i)` with
			// `i < diffs_count` is in bounds; ExternCDiff is Copy, so the stores drop nothing. The
			// writes go through the raw pointer because a reference to the block would be invalid
			// until every `diff_type` discriminant is written.
			unsafe {
				for (i, diff) in change.diffs.iter().enumerate() {
					let marshalled = self.marshal_diff(diff);
					*diffs_array.add(i) = marshalled;
				}
			}

			diffs_array
		} else {
			ptr::null_mut()
		};

		ExternCChange {
			origin: Self::marshal_origin(&change.origin),
			diff_count: diffs_count,
			diffs: diffs_ptr,
			version: change.version.0,
			changed_at: change.changed_at.to_nanos(),
		}
	}

	fn marshal_origin(origin: &ChangeOrigin) -> ExternCOrigin {
		match origin {
			ChangeOrigin::Flow(operator_id) => ExternCOrigin {
				origin: 0,
				id: operator_id.0,
			},
			ChangeOrigin::Object(object_id) => match object_id {
				ObjectId::Table(id) => ExternCOrigin {
					origin: 1,
					id: id.0,
				},
				ObjectId::View(id) => ExternCOrigin {
					origin: 2,
					id: id.0,
				},
				ObjectId::TableVirtual(id) => ExternCOrigin {
					origin: 3,
					id: id.0,
				},
				ObjectId::RingBuffer(id) => ExternCOrigin {
					origin: 4,
					id: id.0,
				},
				ObjectId::Dictionary(id) => ExternCOrigin {
					origin: 6,
					id: id.0,
				},
				ObjectId::Series(id) => ExternCOrigin {
					origin: 7,
					id: id.0,
				},
				ObjectId::Queue(id) => ExternCOrigin {
					origin: 8,
					id: id.0,
				},
			},
		}
	}

	#[instrument(name = "flow::marshal::diff", level = "trace", skip_all, fields(diff_type = ?diff.kind()))]
	fn marshal_diff(&mut self, diff: &Diff) -> ExternCDiff {
		match diff {
			Diff::Insert {
				post,
				..
			} => ExternCDiff {
				diff_type: DiffType::Insert,
				pre: ExternCColumns::empty(),
				post: self.marshal_columns(post),
			},
			Diff::Update {
				pre,
				post,
				..
			} => ExternCDiff {
				diff_type: DiffType::Update,
				pre: self.marshal_columns(pre),
				post: self.marshal_columns(post),
			},
			Diff::Remove {
				pre,
				..
			} => ExternCDiff {
				diff_type: DiffType::Remove,
				pre: self.marshal_columns(pre),
				post: ExternCColumns::empty(),
			},
		}
	}

	pub fn unmarshal_change(&self, extern_c: &ExternCChange) -> Result<Change, String> {
		let mut diffs: Diffs = Diffs::with_capacity(extern_c.diff_count);

		if !extern_c.diffs.is_null() && extern_c.diff_count > 0 {
			// SAFETY: the branch above rules out a null pointer and a zero count; `marshal_change`
			// points `diffs` at an 8-aligned arena array of exactly `diff_count` initialised `ExternCDiff`.
			unsafe {
				let diffs_slice = from_raw_parts(extern_c.diffs, extern_c.diff_count);

				for diff in diffs_slice {
					diffs.push(self.unmarshal_diff(diff)?);
				}
			}
		}

		Ok(Change {
			origin: Self::unmarshal_origin(&extern_c.origin)?,
			diffs,
			version: CommitVersion(extern_c.version),
			changed_at: DateTime::from_nanos(extern_c.changed_at),
		})
	}

	fn unmarshal_origin(extern_c: &ExternCOrigin) -> Result<ChangeOrigin, String> {
		match extern_c.origin {
			0 => Ok(ChangeOrigin::Flow(OperatorId(extern_c.id))),
			1 => Ok(ChangeOrigin::Object(ObjectId::Table(TableId(extern_c.id)))),
			2 => Ok(ChangeOrigin::Object(ObjectId::View(ViewId(extern_c.id)))),
			3 => Ok(ChangeOrigin::Object(ObjectId::TableVirtual(VTableId(extern_c.id)))),
			4 => Ok(ChangeOrigin::Object(ObjectId::RingBuffer(RingBufferId(extern_c.id)))),
			6 => Ok(ChangeOrigin::Object(ObjectId::Dictionary(DictionaryId(extern_c.id)))),
			7 => Ok(ChangeOrigin::Object(ObjectId::Series(SeriesId(extern_c.id)))),
			8 => Ok(ChangeOrigin::Object(ObjectId::Queue(QueueId(extern_c.id)))),
			_ => Err(format!("Invalid origin_type: {}", extern_c.origin)),
		}
	}

	fn unmarshal_diff(&self, extern_c: &ExternCDiff) -> Result<Diff, String> {
		match extern_c.diff_type {
			DiffType::Insert => {
				if extern_c.post.is_empty() {
					return Err("Insert diff missing post columns".to_string());
				}

				let post = self.unmarshal_columns(&extern_c.post);
				Ok(Diff::insert(post))
			}
			DiffType::Update => {
				if extern_c.pre.is_empty() || extern_c.post.is_empty() {
					return Err("Update diff missing pre or post columns".to_string());
				}

				let pre = self.unmarshal_columns(&extern_c.pre);
				let post = self.unmarshal_columns(&extern_c.post);
				Ok(Diff::update(pre, post))
			}
			DiffType::Remove => {
				if extern_c.pre.is_empty() {
					return Err("Remove diff missing pre columns".to_string());
				}

				let pre = self.unmarshal_columns(&extern_c.pre);
				Ok(Diff::remove(pre))
			}
		}
	}
}
