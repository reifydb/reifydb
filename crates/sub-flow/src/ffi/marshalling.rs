//! Type marshalling between Rust and FFI types

use reifydb_operator_abi::*;
use reifydb_core::{
    Row,
    interface::FlowNodeId,
    CommitVersion,
    CowVec,
    value::encoded::{EncodedValues, EncodedValuesNamedLayout},
};
use reifydb_type::{RowNumber, Value};
use crate::flow::{FlowChange, FlowDiff, FlowChangeOrigin};
use crate::ffi::Arena;
use std::collections::HashMap;
use std::ffi::c_void;

/// Marshaller for converting between Rust and FFI types
pub struct FFIMarshaller {
    arena: Arena,
}

impl FFIMarshaller {
    /// Create a new marshaller
    pub fn new() -> Self {
        Self {
            arena: Arena::new(),
        }
    }

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
            version: change.version.into(),
        }
    }

    /// Marshal a single flow diff
    fn marshal_flow_diff(&mut self, diff: &FlowDiff) -> FlowDiffFFI {
        match diff {
            FlowDiff::Insert { post } => FlowDiffFFI {
                diff_type: FlowDiffType::Insert,
                pre_row: std::ptr::null(),
                post_row: self.marshal_row(post),
            },
            FlowDiff::Update { pre, post } => FlowDiffFFI {
                diff_type: FlowDiffType::Update,
                pre_row: self.marshal_row(pre),
                post_row: self.marshal_row(post),
            },
            FlowDiff::Remove { pre } => FlowDiffFFI {
                diff_type: FlowDiffType::Remove,
                pre_row: self.marshal_row(pre),
                post_row: std::ptr::null(),
            },
        }
    }

    /// Unmarshal a flow change from FFI representation
    pub fn unmarshal_flow_change(&self, ffi: &FlowChangeFFI) -> crate::Result<FlowChange> {
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
            origin: FlowChangeOrigin::Internal(FlowNodeId(0)), // TODO: Properly track origin
            diffs,
            version: CommitVersion::from(ffi.version),
        })
    }

    /// Unmarshal a single flow diff
    fn unmarshal_flow_diff(&self, ffi: &FlowDiffFFI) -> crate::Result<FlowDiff> {
        match ffi.diff_type {
            FlowDiffType::Insert => {
                if ffi.post_row.is_null() {
                    return Err(crate::ffi::FFIError::InvalidInput("Insert diff missing post row".to_string()).into());
                }

                let post = unsafe { self.unmarshal_row(&*ffi.post_row) };
                Ok(FlowDiff::Insert { post })
            }
            FlowDiffType::Update => {
                if ffi.pre_row.is_null() || ffi.post_row.is_null() {
                    return Err(crate::ffi::FFIError::InvalidInput("Update diff missing pre or post row".to_string()).into());
                }

                let pre = unsafe { self.unmarshal_row(&*ffi.pre_row) };
                let post = unsafe { self.unmarshal_row(&*ffi.post_row) };
                Ok(FlowDiff::Update { pre, post })
            }
            FlowDiffType::Remove => {
                if ffi.pre_row.is_null() {
                    return Err(crate::ffi::FFIError::InvalidInput("Remove diff missing pre row".to_string()).into());
                }

                let pre = unsafe { self.unmarshal_row(&*ffi.pre_row) };
                Ok(FlowDiff::Remove { pre })
            }
        }
    }

    /// Marshal a row to FFI representation
    pub fn marshal_row(&mut self, row: &Row) -> *const RowFFI {
        // Allocate RowFFI in arena
        let row_ffi = self.arena.alloc_type::<RowFFI>();

        if row_ffi.is_null() {
            return std::ptr::null();
        }

        // Copy encoded data to arena
        let encoded_ptr = self.arena.copy_bytes(row.encoded.as_ref());
        let encoded_len = row.encoded.len();

        // Store layout as opaque pointer (we'll keep a reference in host)
        // In a real implementation, we'd need a way to manage layout handles
        let layout_handle = Box::into_raw(Box::new(row.layout.clone())) as *const c_void;

        unsafe {
            *row_ffi = RowFFI {
                number: row.number.into(),
                encoded: BufferFFI {
                    ptr: encoded_ptr,
                    len: encoded_len,
                    cap: encoded_len,
                },
                layout_handle,
            };
        }

        row_ffi as *const RowFFI
    }

    /// Unmarshal a row from FFI representation
    pub fn unmarshal_row(&self, ffi: &RowFFI) -> Row {
        // Extract encoded data
        let encoded = if !ffi.encoded.ptr.is_null() && ffi.encoded.len > 0 {
            unsafe {
                let slice = std::slice::from_raw_parts(ffi.encoded.ptr, ffi.encoded.len);
                EncodedValues(CowVec::new(slice.to_vec()))
            }
        } else {
            EncodedValues(CowVec::new(Vec::new()))
        };

        // Extract layout (in a real implementation, we'd lookup from handle)
        let layout = if !ffi.layout_handle.is_null() {
            unsafe {
                let layout_box = Box::from_raw(ffi.layout_handle as *mut EncodedValuesNamedLayout);
                let layout = (*layout_box).clone();
                let _ = Box::into_raw(layout_box); // Don't deallocate, just borrow
                layout
            }
        } else {
            // Create an empty layout with no fields
            EncodedValuesNamedLayout::new(std::iter::empty())
        };

        Row {
            number: RowNumber::from(ffi.number),
            encoded,
            layout,
        }
    }

    /// Clear the arena
    pub fn clear(&mut self) {
        self.arena.clear();
    }
}

impl Default for FFIMarshaller {
    fn default() -> Self {
        Self::new()
    }
}