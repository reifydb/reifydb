// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow change marshalling between Rust and FFI types

use std::slice::{from_raw_parts, from_raw_parts_mut};

use reifydb_abi::{
	data::column::ColumnsFFI,
	flow::{
		change::{FlowChangeFFI, FlowOriginFFI},
		diff::{FlowDiffFFI, FlowDiffType},
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{RingBufferId, TableId, ViewId},
		primitive::PrimitiveId,
		vtable::VTableId,
	},
};

use crate::{
	ffi::arena::Arena,
	flow::{FlowChange, FlowChangeOrigin, FlowDiff},
};

impl Arena {
	/// Marshal a flow change to FFI representation
	pub fn marshal_flow_change(&mut self, change: &FlowChange) -> FlowChangeFFI {
		// Allocate array for diffs
		let diffs_count = change.diffs.len();
		let diffs_ptr = if diffs_count > 0 {
			let diffs_array = self.alloc(diffs_count * size_of::<FlowDiffFFI>()) as *mut FlowDiffFFI;

			// Marshal each diff
			unsafe {
				let diffs_slice = from_raw_parts_mut(diffs_array, diffs_count);
				for (i, diff) in change.diffs.iter().enumerate() {
					diffs_slice[i] = self.marshal_flow_diff(diff);
				}
			}

			diffs_array
		} else {
			std::ptr::null_mut()
		};

		FlowChangeFFI {
			origin: Self::marshal_origin(&change.origin),
			diff_count: diffs_count,
			diffs: diffs_ptr,
			version: change.version.0,
		}
	}

	/// Marshal a flow change origin to FFI representation
	fn marshal_origin(origin: &FlowChangeOrigin) -> FlowOriginFFI {
		match origin {
			FlowChangeOrigin::Internal(node_id) => FlowOriginFFI {
				origin: 0,
				id: node_id.0,
			},
			FlowChangeOrigin::External(source_id) => match source_id {
				PrimitiveId::Table(id) => FlowOriginFFI {
					origin: 1,
					id: id.0,
				},
				PrimitiveId::View(id) => FlowOriginFFI {
					origin: 2,
					id: id.0,
				},
				PrimitiveId::TableVirtual(id) => FlowOriginFFI {
					origin: 3,
					id: id.0,
				},
				PrimitiveId::RingBuffer(id) => FlowOriginFFI {
					origin: 4,
					id: id.0,
				},
				&PrimitiveId::Flow(id) => FlowOriginFFI {
					origin: 5,
					id: id.0,
				},
				PrimitiveId::Dictionary(id) => FlowOriginFFI {
					origin: 6,
					id: id.0,
				},
			},
		}
	}

	/// Marshal a single flow diff using columnar format
	fn marshal_flow_diff(&mut self, diff: &FlowDiff) -> FlowDiffFFI {
		match diff {
			FlowDiff::Insert {
				post,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Insert,
				pre: ColumnsFFI::empty(),
				post: self.marshal_columns(post),
			},
			FlowDiff::Update {
				pre,
				post,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Update,
				pre: self.marshal_columns(pre),
				post: self.marshal_columns(post),
			},
			FlowDiff::Remove {
				pre,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Remove,
				pre: self.marshal_columns(pre),
				post: ColumnsFFI::empty(),
			},
		}
	}

	/// Unmarshal a flow change from FFI representation
	pub fn unmarshal_flow_change(&self, ffi: &FlowChangeFFI) -> Result<FlowChange, String> {
		let mut diffs = Vec::with_capacity(ffi.diff_count);

		if !ffi.diffs.is_null() && ffi.diff_count > 0 {
			unsafe {
				let diffs_slice = from_raw_parts(ffi.diffs, ffi.diff_count);

				for diff_ffi in diffs_slice {
					diffs.push(self.unmarshal_flow_diff(diff_ffi)?);
				}
			}
		}

		Ok(FlowChange {
			origin: Self::unmarshal_origin(&ffi.origin)?,
			diffs,
			version: CommitVersion(ffi.version),
		})
	}

	/// Unmarshal a flow change origin from FFI representation
	fn unmarshal_origin(ffi: &FlowOriginFFI) -> Result<FlowChangeOrigin, String> {
		match ffi.origin {
			0 => Ok(FlowChangeOrigin::Internal(FlowNodeId(ffi.id))),
			1 => Ok(FlowChangeOrigin::External(PrimitiveId::Table(TableId(ffi.id)))),
			2 => Ok(FlowChangeOrigin::External(PrimitiveId::View(ViewId(ffi.id)))),
			3 => Ok(FlowChangeOrigin::External(PrimitiveId::TableVirtual(VTableId(ffi.id)))),
			4 => Ok(FlowChangeOrigin::External(PrimitiveId::RingBuffer(RingBufferId(ffi.id)))),
			5 => Ok(FlowChangeOrigin::External(PrimitiveId::Flow(FlowId(ffi.id)))),
			_ => Err(format!("Invalid origin_type: {}", ffi.origin)),
		}
	}

	/// Unmarshal a single flow diff from columnar FFI format
	fn unmarshal_flow_diff(&self, ffi: &FlowDiffFFI) -> Result<FlowDiff, String> {
		match ffi.diff_type {
			FlowDiffType::Insert => {
				if ffi.post.is_empty() {
					return Err("Insert diff missing post columns".to_string());
				}

				let post = self.unmarshal_columns(&ffi.post);
				Ok(FlowDiff::Insert {
					post,
				})
			}
			FlowDiffType::Update => {
				if ffi.pre.is_empty() || ffi.post.is_empty() {
					return Err("Update diff missing pre or post columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				let post = self.unmarshal_columns(&ffi.post);
				Ok(FlowDiff::Update {
					pre,
					post,
				})
			}
			FlowDiffType::Remove => {
				if ffi.pre.is_empty() {
					return Err("Remove diff missing pre columns".to_string());
				}

				let pre = self.unmarshal_columns(&ffi.pre);
				Ok(FlowDiff::Remove {
					pre,
				})
			}
		}
	}
}
