//! Transaction handle for FFI operators

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::HostCallbacks;

use crate::transaction::FlowTransaction;

/// Alias for the ABI TransactionHandle
pub type TransactionHandle = reifydb_flow_operator_abi::TransactionHandle;

/// Helper functions for TransactionHandle
pub trait TransactionHandleExt {
	/// Create a new transaction handle
	fn new(txn: &mut FlowTransaction, operator_id: FlowNodeId, callbacks: HostCallbacks) -> Self;
	/// Get mutable reference to the transaction (unsafe - caller must ensure validity)
	unsafe fn get_transaction_mut(&mut self) -> &mut FlowTransaction;
}

impl TransactionHandleExt for TransactionHandle {
	fn new(txn: &mut FlowTransaction, operator_id: FlowNodeId, callbacks: HostCallbacks) -> Self {
		Self {
			txn_ptr: txn as *mut _ as *mut core::ffi::c_void,
			operator_id: operator_id.0,
			callbacks,
		}
	}

	unsafe fn get_transaction_mut(&mut self) -> &mut FlowTransaction {
		unsafe { &mut *(self.txn_ptr as *mut FlowTransaction) }
	}
}
