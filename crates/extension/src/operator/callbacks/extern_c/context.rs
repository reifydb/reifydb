// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;
use std::marker::PhantomData;

use reifydb_core::{common::OperatorClass, interface::catalog::flow::OperatorId};
use reifydb_flow_async::operator::host::HostContext;
use reifydb_sdk::{
	common::extern_c::wire::status::EXTERN_C_ERROR_INTERNAL,
	flow::operator::extern_c::wire::{callbacks::OperatorCallbacks, context::ExternCContextRaw},
};
use reifydb_value::error::Error;

pub struct ExternCHostContext<'a> {
	host: *mut (dyn HostContext + 'a),
	class: OperatorClass,
	failure: Option<Error>,
	_marker: PhantomData<&'a mut (dyn HostContext + 'a)>,
}

impl<'a> ExternCHostContext<'a> {
	pub fn new(host: &'a mut (dyn HostContext + 'a), class: OperatorClass) -> Self {
		Self {
			host: host as *mut (dyn HostContext + 'a),
			class,
			failure: None,
			_marker: PhantomData,
		}
	}

	pub fn take_failure(&mut self) -> Option<Error> {
		self.failure.take()
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

/// # Safety
///
/// Same contract as [`get_host_mut`]: `ctx.txn_ptr` must be the pointer stored by [`new_extern_c_context`] from an
/// [`ExternCHostContext`] that is still alive.
pub(crate) unsafe fn guest_class(ctx: &ExternCContextRaw) -> OperatorClass {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned ExternCHostContext.
	unsafe { (*(ctx.txn_ptr as *const ExternCHostContext<'_>)).class }
}

/// # Safety
///
/// Same contract as [`get_host_mut`]: `ctx.txn_ptr` must be the pointer stored by [`new_extern_c_context`] from an
/// [`ExternCHostContext`] that is still alive, and no reference to that [`ExternCHostContext`] itself may be live.
pub(crate) unsafe fn stash_failure(ctx: &ExternCContextRaw, err: Error) -> i32 {
	// SAFETY: discharges this function's own contract; the place borrow covers only the `failure` field, which is
	// disjoint from the HostContext a live get_host_mut borrow points at through the `host` field.
	let failure = unsafe { &mut (*(ctx.txn_ptr as *mut ExternCHostContext<'_>)).failure };
	if failure.is_none() {
		*failure = Some(err);
	}
	EXTERN_C_ERROR_INTERNAL
}
