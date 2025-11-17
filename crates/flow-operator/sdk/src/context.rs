//! Operator context providing access to state and resources

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::FFIContext;

use crate::stateful::State;

/// Operator context providing access to state and other resources
pub struct OperatorContext {
	pub(crate) ctx: *mut FFIContext,
}

impl OperatorContext {
	/// Create a new operator context from an FFI context pointer
	///
	/// # Safety
	/// The caller must ensure ctx is non-null and valid for the lifetime of this context
	pub fn new(ctx: *mut FFIContext) -> Self {
		assert!(!ctx.is_null(), "FFIContext pointer must not be null");
		Self {
			ctx,
		}
	}

	/// Get the operator ID from the FFI context
	pub fn operator_id(&self) -> FlowNodeId {
		unsafe { FlowNodeId((*self.ctx).operator_id) }
	}

	/// Get a state manager
	pub fn state(&mut self) -> State<'_> {
		State::new(self)
	}
}
