// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_abi::{callbacks::host::HostCallbacks, context::context::ContextFFI};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_engine::vm::executor::Executor;
use reifydb_flow::transaction::FlowTransaction;

pub(crate) fn new_ffi_context(
	txn: &mut FlowTransaction,
	executor: &Executor,
	operator_id: FlowNodeId,
	callbacks: HostCallbacks,
) -> ContextFFI {
	let clock_now_nanos = txn.clock().now().to_nanos();
	let state_lease_bytes =
		txn.state_budget().current_lease(operator_id).map(|lease| lease.grant.bytes().as_bytes()).unwrap_or(0);
	ContextFFI {
		txn_ptr: txn as *mut _ as *mut c_void,
		executor_ptr: executor as *const _ as *const c_void,
		operator_id: operator_id.0,
		clock_now_nanos,
		state_lease_bytes,
		callbacks,
	}
}

/// # Safety
///
/// `ctx.txn_ptr` must be the pointer stored by [`new_ffi_context`] from a live
/// `&mut FlowTransaction` that is still borrowed exclusively, so the returned
/// reference is the only one aliasing it for its lifetime.
pub(crate) unsafe fn get_transaction_mut(ctx: &mut ContextFFI) -> &mut FlowTransaction {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned
	// FlowTransaction that nothing else aliases for the returned lifetime.
	unsafe { &mut *(ctx.txn_ptr as *mut FlowTransaction) }
}
