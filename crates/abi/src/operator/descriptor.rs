use super::{OperatorColumnDefsFFI, OperatorVTableFFI};
use crate::data::BufferFFI;

/// Descriptor for an FFI operator
///
/// This structure describes an operator's capabilities and provides
/// its virtual function table.
#[repr(C)]
pub struct OperatorDescriptorFFI {
	/// API version (must match CURRENT_API)
	pub api: u32,

	/// Operator name (UTF-8 encoded)
	pub operator: BufferFFI,

	/// Semantic version (UTF-8 encoded, e.g., "1.0.0")
	pub version: BufferFFI,

	/// Description (UTF-8 encoded)
	pub description: BufferFFI,

	/// Input columns describing expected input row format (for documentation)
	pub input_columns: OperatorColumnDefsFFI,

	/// Output columns describing output row format (for documentation)
	pub output_columns: OperatorColumnDefsFFI,

	/// Capabilities bitflags (CAPABILITY_* constants)
	pub capabilities: u32,

	/// Virtual function table with all operator methods
	pub vtable: OperatorVTableFFI,
}

// SAFETY: OperatorDescriptorFFI contains pointers to static strings and functions
// which are safe to share across threads
unsafe impl Send for OperatorDescriptorFFI {}
unsafe impl Sync for OperatorDescriptorFFI {}
