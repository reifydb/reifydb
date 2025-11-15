//! Transaction handle for FFI operators

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::HostCallbacks;

use crate::transaction::FlowTransaction;

/// Handle for passing transaction context to FFI operators
pub struct TransactionHandle {
	pub(crate) txn_ptr: *mut FlowTransaction,
	pub(crate) operator_id: FlowNodeId,
	pub(crate) callbacks: HostCallbacks,
}

// SAFETY: TransactionHandle is used only within single-threaded FFI calls
unsafe impl Send for TransactionHandle {}
unsafe impl Sync for TransactionHandle {}

impl TransactionHandle {
	/// Create a new transaction handle
	pub fn new(txn: &mut FlowTransaction, operator_id: FlowNodeId, callbacks: HostCallbacks) -> Self {
		Self {
			txn_ptr: txn as *mut FlowTransaction,
			operator_id,
			callbacks,
		}
	}

	/// Get the operator ID
	pub fn operator_id(&self) -> FlowNodeId {
		self.operator_id
	}
}
