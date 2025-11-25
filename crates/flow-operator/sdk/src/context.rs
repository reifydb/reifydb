//! Operator context providing access to state and resources

use reifydb_core::{EncodedKey, interface::FlowNodeId};
use reifydb_flow_operator_abi::FFIContext;
use reifydb_type::RowNumber;

use crate::{
	stateful::{RowNumberProvider, State},
	store::Store,
};

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

	/// Get read-only access to the underlying store
	pub fn store(&mut self) -> Store<'_> {
		Store::new(self)
	}

	/// Get or create a row number for a given key
	///
	/// This is a convenience method that creates a RowNumberProvider and
	/// delegates to its `get_or_create_row_number` method.
	///
	/// Returns `(RowNumber, is_new)` where `is_new` indicates if this is
	/// a newly created row number.
	/// ```
	pub fn get_or_create_row_number(&mut self, key: &EncodedKey) -> crate::Result<(RowNumber, bool)> {
		let provider = RowNumberProvider::new(self.operator_id());
		provider.get_or_create_row_number(self, key)
	}
}
