// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow change marshalling between Rust and FFI types

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
			primitive::PrimitiveId,
			vtable::VTableId,
		},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_type::value::dictionary::DictionaryId;

use crate::ffi::arena::Arena;

impl Arena {
	/// Marshal a change to FFI representation
	pub fn marshal_change(&mut self, change: &Change) -> ChangeFFI {
		// Allocate array for diffs
		let diffs_count = change.diffs.len();
		let diffs_ptr = if diffs_count > 0 {
			let diffs_array = self.alloc(diffs_count * size_of::<DiffFFI>()) as *mut DiffFFI;

			// Marshal each diff
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
		}
	}

	/// Marshal a change origin to FFI representation
	fn marshal_origin(origin: &ChangeOrigin) -> OriginFFI {
		match origin {
			ChangeOrigin::Flow(node_id) => OriginFFI {
				origin: 0,
				id: node_id.0,
			},
			ChangeOrigin::Primitive(source_id) => match source_id {
				PrimitiveId::Table(id) => OriginFFI {
					origin: 1,
					id: id.0,
				},
				PrimitiveId::View(id) => OriginFFI {
					origin: 2,
					id: id.0,
				},
				PrimitiveId::TableVirtual(id) => OriginFFI {
					origin: 3,
					id: id.0,
				},
				PrimitiveId::RingBuffer(id) => OriginFFI {
					origin: 4,
					id: id.0,
				},
				PrimitiveId::Dictionary(id) => OriginFFI {
					origin: 6,
					id: id.0,
				},
				PrimitiveId::Series(id) => OriginFFI {
					origin: 7,
					id: id.0,
				},
			},
		}
	}

	/// Marshal a single diff using columnar format
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

	/// Unmarshal a change from FFI representation
	pub fn unmarshal_change(&self, ffi: &ChangeFFI) -> Result<Change, String> {
		let mut diffs = Vec::with_capacity(ffi.diff_count);

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
		})
	}

	/// Unmarshal a change origin from FFI representation
	fn unmarshal_origin(ffi: &OriginFFI) -> Result<ChangeOrigin, String> {
		match ffi.origin {
			0 => Ok(ChangeOrigin::Flow(FlowNodeId(ffi.id))),
			1 => Ok(ChangeOrigin::Primitive(PrimitiveId::Table(TableId(ffi.id)))),
			2 => Ok(ChangeOrigin::Primitive(PrimitiveId::View(ViewId(ffi.id)))),
			3 => Ok(ChangeOrigin::Primitive(PrimitiveId::TableVirtual(VTableId(ffi.id)))),
			4 => Ok(ChangeOrigin::Primitive(PrimitiveId::RingBuffer(RingBufferId(ffi.id)))),
			6 => Ok(ChangeOrigin::Primitive(PrimitiveId::Dictionary(DictionaryId(ffi.id)))),
			7 => Ok(ChangeOrigin::Primitive(PrimitiveId::Series(SeriesId(ffi.id)))),
			_ => Err(format!("Invalid origin_type: {}", ffi.origin)),
		}
	}

	/// Unmarshal a single diff from columnar FFI format
	fn unmarshal_diff(&self, ffi: &DiffFFI) -> Result<Diff, String> {
		match ffi.diff_type {
			DiffType::Insert => {
				if ffi.post.is_empty() {
					return Err("Insert diff missing post columns".to_string());
				}

				let post = self.unmarshal_columns(&ffi.post);
				Ok(Diff::Insert {
					post,
				})
			}
			DiffType::Update => {
				if ffi.pre.is_empty() || ffi.post.is_empty() {
					return Err("Update diff missing pre or post columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				let post = self.unmarshal_columns(&ffi.post);
				Ok(Diff::Update {
					pre,
					post,
				})
			}
			DiffType::Remove => {
				if ffi.pre.is_empty() {
					return Err("Remove diff missing pre columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				Ok(Diff::Remove {
					pre,
				})
			}
		}
	}
}
