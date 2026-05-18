// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use reifydb_abi::{callbacks::host::HostCallbacks, context::context::ContextFFI};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_engine::vm::executor::Executor;

use crate::transaction::FlowTransaction;

pub(crate) fn new_ffi_context(
	txn: &mut FlowTransaction,
	executor: &Executor,
	operator_id: FlowNodeId,
	callbacks: HostCallbacks,
) -> ContextFFI {
	let clock_now_nanos = txn.clock().now_nanos();
	ContextFFI {
		txn_ptr: txn as *mut _ as *mut c_void,
		executor_ptr: executor as *const _ as *const c_void,
		operator_id: operator_id.0,
		clock_now_nanos,
		callbacks,
	}
}

pub(crate) unsafe fn get_transaction_mut(ctx: &mut ContextFFI) -> &mut FlowTransaction {
	unsafe { &mut *(ctx.txn_ptr as *mut FlowTransaction) }
}
