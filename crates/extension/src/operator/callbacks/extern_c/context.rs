// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;
use std::marker::PhantomData;

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_flow::operator::bridge::Bridge;
use reifydb_sdk::flow::operator::extern_c::wire::{callbacks::OperatorCallbacks, context::ExternCContext};

pub struct ExternCBridge<'a> {
	bridge: *mut (dyn Bridge + 'a),
	_marker: PhantomData<&'a mut (dyn Bridge + 'a)>,
}

impl<'a> ExternCBridge<'a> {
	pub fn new(bridge: &'a mut (dyn Bridge + 'a)) -> Self {
		Self {
			bridge: bridge as *mut (dyn Bridge + 'a),
			_marker: PhantomData,
		}
	}

	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut Self as *mut c_void
	}
}

pub fn new_extern_c_context(
	bridge: &mut ExternCBridge<'_>,
	operator_id: OperatorId,
	callbacks: OperatorCallbacks,
) -> ExternCContext {
	// SAFETY: `bridge` holds the &'a mut dyn Bridge ExternCBridge::new was built from; PhantomData keeps
	// that borrow live for 'a and &mut self makes this deref unique.
	let written_at_nanos = unsafe { (*bridge.bridge).written_at() }.to_nanos();
	ExternCContext {
		txn_ptr: bridge.as_ptr(),
		written_at_nanos,
		operator_id: operator_id.0,
		callbacks,
	}
}

/// # Safety
///
/// `ctx.txn_ptr` must be the pointer stored by [`new_extern_c_context`] from an [`ExternCBridge`]
/// that is still alive and still holds its exclusive `&mut dyn Bridge`, so the returned reference
/// is the only one aliasing it for its lifetime.
pub(crate) unsafe fn get_bridge_mut<'a>(ctx: &mut ExternCContext) -> &'a mut dyn Bridge {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned
	// ExternCBridge whose inner fat pointer nothing else aliases for the returned lifetime.
	unsafe { &mut *(*(ctx.txn_ptr as *mut ExternCBridge<'a>)).bridge }
}
