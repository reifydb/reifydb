//! FFI context utilities for operators

use core::ffi::c_void;

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{FFIContext, HostCallbacks};

use crate::transaction::FlowTransaction;

/// Create a new FFI context
pub(crate) fn new_ffi_context(
	txn: &mut FlowTransaction,
	operator_id: FlowNodeId,
	callbacks: HostCallbacks,
) -> FFIContext {
	FFIContext {
		txn_ptr: txn as *mut _ as *mut c_void,
		operator_id: operator_id.0,
		callbacks,
	}
}

/// Get mutable reference to the transaction from an FFI context
///
/// # Safety
/// Caller must ensure the context's txn_ptr is valid and points to a FlowTransaction
pub(crate) unsafe fn get_transaction_mut(ctx: &mut FFIContext) -> &mut FlowTransaction {
	unsafe { &mut *(ctx.txn_ptr as *mut FlowTransaction) }
}
