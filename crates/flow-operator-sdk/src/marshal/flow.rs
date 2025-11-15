//! Flow change marshalling between Rust and FFI types

use reifydb_flow_operator_abi::*;

use crate::{
	marshal::Marshaller,
	operator::{FlowChange, FlowDiff},
};

impl Marshaller {
	/// Marshal a flow change to FFI representation
	pub fn marshal_flow_change(&mut self, change: &FlowChange) -> FlowChangeFFI {
		// Allocate array for diffs
		let diffs_count = change.diffs.len();
		let diffs_ptr = if diffs_count > 0 {
			let diffs_array = self.arena.alloc(diffs_count * size_of::<FlowDiffFFI>()) as *mut FlowDiffFFI;

			// Marshal each diff
			unsafe {
				let diffs_slice = std::slice::from_raw_parts_mut(diffs_array, diffs_count);

				for (i, diff) in change.diffs.iter().enumerate() {
					diffs_slice[i] = self.marshal_flow_diff(diff);
				}
			}

			diffs_array
		} else {
			std::ptr::null_mut()
		};

		FlowChangeFFI {
			diff_count: diffs_count,
			diffs: diffs_ptr,
			version: change.version,
		}
	}

	/// Marshal a single flow diff
	fn marshal_flow_diff(&mut self, diff: &FlowDiff) -> FlowDiffFFI {
		match diff {
			FlowDiff::Insert {
				post,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Insert,
				pre_row: std::ptr::null(),
				post_row: self.marshal_row(post),
			},
			FlowDiff::Update {
				pre,
				post,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Update,
				pre_row: self.marshal_row(pre),
				post_row: self.marshal_row(post),
			},
			FlowDiff::Remove {
				pre,
			} => FlowDiffFFI {
				diff_type: FlowDiffType::Remove,
				pre_row: self.marshal_row(pre),
				post_row: std::ptr::null(),
			},
		}
	}

	/// Unmarshal a flow change from FFI representation
	pub fn unmarshal_flow_change(&self, ffi: &FlowChangeFFI) -> Result<FlowChange, String> {
		let mut diffs = Vec::with_capacity(ffi.diff_count);

		if !ffi.diffs.is_null() && ffi.diff_count > 0 {
			unsafe {
				let diffs_slice = std::slice::from_raw_parts(ffi.diffs, ffi.diff_count);

				for diff_ffi in diffs_slice {
					diffs.push(self.unmarshal_flow_diff(diff_ffi)?);
				}
			}
		}

		Ok(FlowChange {
			diffs,
			version: ffi.version,
		})
	}

	/// Unmarshal a single flow diff
	fn unmarshal_flow_diff(&self, ffi: &FlowDiffFFI) -> Result<FlowDiff, String> {
		match ffi.diff_type {
			FlowDiffType::Insert => {
				if ffi.post_row.is_null() {
					return Err("Insert diff missing post row".to_string());
				}

				let post = unsafe { self.unmarshal_row(&*ffi.post_row) };
				Ok(FlowDiff::Insert {
					post,
				})
			}
			FlowDiffType::Update => {
				if ffi.pre_row.is_null() || ffi.post_row.is_null() {
					return Err("Update diff missing pre or post row".to_string());
				}

				let pre = unsafe { self.unmarshal_row(&*ffi.pre_row) };
				let post = unsafe { self.unmarshal_row(&*ffi.post_row) };
				Ok(FlowDiff::Update {
					pre,
					post,
				})
			}
			FlowDiffType::Remove => {
				if ffi.pre_row.is_null() {
					return Err("Remove diff missing pre row".to_string());
				}

				let pre = unsafe { self.unmarshal_row(&*ffi.pre_row) };
				Ok(FlowDiff::Remove {
					pre,
				})
			}
		}
	}
}
