// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;
use std::marker::PhantomData;

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_flow::operator::host::HostContext;
use reifydb_sdk::flow::operator::extern_c::wire::{callbacks::OperatorCallbacks, context::ExternCContextRaw};

pub struct ExternCHostContext<'a> {
	host: *mut (dyn HostContext + 'a),
	_marker: PhantomData<&'a mut (dyn HostContext + 'a)>,
}

impl<'a> ExternCHostContext<'a> {
	pub fn new(host: &'a mut (dyn HostContext + 'a)) -> Self {
		Self {
			host: host as *mut (dyn HostContext + 'a),
			_marker: PhantomData,
		}
	}

	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut Self as *mut c_void
	}
}

pub fn new_extern_c_context(
	host: &mut ExternCHostContext<'_>,
	operator_id: OperatorId,
	callbacks: OperatorCallbacks,
) -> ExternCContextRaw {
	// SAFETY: `host` holds the &'a mut dyn HostContext ExternCHostContext::new was built from; PhantomData keeps
	// that borrow live for 'a and &mut self makes this deref unique.
	let written_at_nanos = unsafe { (*host.host).written_at() }.to_nanos();
	ExternCContextRaw {
		txn_ptr: host.as_ptr(),
		written_at_nanos,
		operator_id: operator_id.0,
		callbacks,
	}
}

/// # Safety
///
/// `ctx.txn_ptr` must be the pointer stored by [`new_extern_c_context`] from an [`ExternCHostContext`]
/// that is still alive and still holds its exclusive `&mut dyn HostContext`, so the returned reference
/// is the only one aliasing it for its lifetime.
pub(crate) unsafe fn get_host_mut<'a>(ctx: &mut ExternCContextRaw) -> &'a mut dyn HostContext {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned
	// ExternCHostContext whose inner fat pointer nothing else aliases for the returned lifetime.
	unsafe { &mut *(*(ctx.txn_ptr as *mut ExternCHostContext<'a>)).host }
}
