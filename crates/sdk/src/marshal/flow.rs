// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	ptr,
	slice::{from_raw_parts, from_raw_parts_mut},
};

use reifydb_abi::{
	data::column::ColumnsFFI,
	flow::{
		change::{ChangeFFI, OriginFFI},
		diff::{DiffFFI, DiffType},
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{RingBufferId, SeriesId, TableId, ViewId},
			shape::ShapeId,
			vtable::VTableId,
		},
		change::{Change, ChangeOrigin, Diff, Diffs},
	},
};
use reifydb_type::value::{datetime::DateTime, dictionary::DictionaryId};

use crate::ffi::arena::Arena;

impl Arena {
	pub fn marshal_change(&mut self, change: &Change) -> ChangeFFI {
		let diffs_count = change.diffs.len();
		let diffs_ptr = if diffs_count > 0 {
			let diffs_array = self.alloc(diffs_count * size_of::<DiffFFI>()) as *mut DiffFFI;

			unsafe {
				let diffs_slice = from_raw_parts_mut(diffs_array, diffs_count);
				for (i, diff) in change.diffs.iter().enumerate() {
					diffs_slice[i] = self.marshal_diff(diff);
				}
			}

			diffs_array
		} else {
			ptr::null_mut()
		};

		ChangeFFI {
			origin: Self::marshal_origin(&change.origin),
			diff_count: diffs_count,
			diffs: diffs_ptr,
			version: change.version.0,
			changed_at: change.changed_at.to_nanos(),
		}
	}

	fn marshal_origin(origin: &ChangeOrigin) -> OriginFFI {
		match origin {
			ChangeOrigin::Flow(node_id) => OriginFFI {
				origin: 0,
				id: node_id.0,
			},
			ChangeOrigin::Shape(shape_id) => match shape_id {
				ShapeId::Table(id) => OriginFFI {
					origin: 1,
					id: id.0,
				},
				ShapeId::View(id) => OriginFFI {
					origin: 2,
					id: id.0,
				},
				ShapeId::TableVirtual(id) => OriginFFI {
					origin: 3,
					id: id.0,
				},
				ShapeId::RingBuffer(id) => OriginFFI {
					origin: 4,
					id: id.0,
				},
				ShapeId::Dictionary(id) => OriginFFI {
					origin: 6,
					id: id.0,
				},
				ShapeId::Series(id) => OriginFFI {
					origin: 7,
					id: id.0,
				},
			},
		}
	}

	fn marshal_diff(&mut self, diff: &Diff) -> DiffFFI {
		match diff {
			Diff::Insert {
				post,
			} => DiffFFI {
				diff_type: DiffType::Insert,
				pre: ColumnsFFI::empty(),
				post: self.marshal_columns(post),
			},
			Diff::Update {
				pre,
				post,
			} => DiffFFI {
				diff_type: DiffType::Update,
				pre: self.marshal_columns(pre),
				post: self.marshal_columns(post),
			},
			Diff::Remove {
				pre,
			} => DiffFFI {
				diff_type: DiffType::Remove,
				pre: self.marshal_columns(pre),
				post: ColumnsFFI::empty(),
			},
		}
	}

	pub fn unmarshal_change(&self, ffi: &ChangeFFI) -> Result<Change, String> {
		let mut diffs: Diffs = Diffs::with_capacity(ffi.diff_count);

		if !ffi.diffs.is_null() && ffi.diff_count > 0 {
			unsafe {
				let diffs_slice = from_raw_parts(ffi.diffs, ffi.diff_count);

				for diff_ffi in diffs_slice {
					diffs.push(self.unmarshal_diff(diff_ffi)?);
				}
			}
		}

		Ok(Change {
			origin: Self::unmarshal_origin(&ffi.origin)?,
			diffs,
			version: CommitVersion(ffi.version),
			changed_at: DateTime::from_nanos(ffi.changed_at),
		})
	}

	fn unmarshal_origin(ffi: &OriginFFI) -> Result<ChangeOrigin, String> {
		match ffi.origin {
			0 => Ok(ChangeOrigin::Flow(FlowNodeId(ffi.id))),
			1 => Ok(ChangeOrigin::Shape(ShapeId::Table(TableId(ffi.id)))),
			2 => Ok(ChangeOrigin::Shape(ShapeId::View(ViewId(ffi.id)))),
			3 => Ok(ChangeOrigin::Shape(ShapeId::TableVirtual(VTableId(ffi.id)))),
			4 => Ok(ChangeOrigin::Shape(ShapeId::RingBuffer(RingBufferId(ffi.id)))),
			6 => Ok(ChangeOrigin::Shape(ShapeId::Dictionary(DictionaryId(ffi.id)))),
			7 => Ok(ChangeOrigin::Shape(ShapeId::Series(SeriesId(ffi.id)))),
			_ => Err(format!("Invalid origin_type: {}", ffi.origin)),
		}
	}

	fn unmarshal_diff(&self, ffi: &DiffFFI) -> Result<Diff, String> {
		match ffi.diff_type {
			DiffType::Insert => {
				if ffi.post.is_empty() {
					return Err("Insert diff missing post columns".to_string());
				}

				let post = self.unmarshal_columns(&ffi.post);
				Ok(Diff::insert(post))
			}
			DiffType::Update => {
				if ffi.pre.is_empty() || ffi.post.is_empty() {
					return Err("Update diff missing pre or post columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				let post = self.unmarshal_columns(&ffi.post);
				Ok(Diff::update(pre, post))
			}
			DiffType::Remove => {
				if ffi.pre.is_empty() {
					return Err("Remove diff missing pre columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				Ok(Diff::remove(pre))
			}
		}
	}
}
