// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_abi::{callbacks::host::HostCallbacks, context::context::ContextFFI};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_engine::vm::executor::Executor;
use reifydb_flow::transaction::DepFlowTransaction;

pub(crate) fn new_ffi_context(
	txn: &mut DepFlowTransaction,
	executor: &Executor,
	operator_id: OperatorId,
	callbacks: HostCallbacks,
) -> ContextFFI {
	let written_at_nanos = txn.written_at().to_nanos();
	ContextFFI {
		txn_ptr: txn as *mut _ as *mut c_void,
		executor_ptr: executor as *const _ as *const c_void,
		operator_id: operator_id.0,
		written_at_nanos,
		callbacks,
	}
}

/// # Safety
///
/// `ctx.txn_ptr` must be the pointer stored by [`new_ffi_context`] from a live
/// `&mut FlowTransaction` that is still borrowed exclusively, so the returned
/// reference is the only one aliasing it for its lifetime.
pub(crate) unsafe fn get_transaction_mut(ctx: &mut ContextFFI) -> &mut DepFlowTransaction {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned
	// FlowTransaction that nothing else aliases for the returned lifetime.
	unsafe { &mut *(ctx.txn_ptr as *mut DepFlowTransaction) }
}
