// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_flow::transaction::{FlowTransaction, deferred::DeferredTransaction};
use reifydb_sdk::flow::operator::extern_c::wire::{callbacks::OperatorCallbacks, context::ExternCContext};

pub fn new_extern_c_context(
	txn: &mut DeferredTransaction,
	operator_id: OperatorId,
	callbacks: OperatorCallbacks,
) -> ExternCContext {
	let written_at_nanos = txn.written_at().to_nanos();
	ExternCContext {
		txn_ptr: txn as *mut _ as *mut c_void,
		written_at_nanos,
		operator_id: operator_id.0,
		callbacks,
	}
}

/// # Safety
///
/// `ctx.txn_ptr` must be the pointer stored by [`new_extern_c_context`] from a live
/// `&mut FlowTransaction` that is still borrowed exclusively, so the returned
/// reference is the only one aliasing it for its lifetime.
pub(crate) unsafe fn get_transaction_mut(ctx: &mut ExternCContext) -> &mut DeferredTransaction {
	// SAFETY: discharges this function's own contract; `ctx.txn_ptr` is then a live, aligned
	// FlowTransaction that nothing else aliases for the returned lifetime.
	unsafe { &mut *(ctx.txn_ptr as *mut DeferredTransaction) }
}
