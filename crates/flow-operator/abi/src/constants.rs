//! Constants and version information for the FFI operator API

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API: u32 = 1;

/// Magic number to identify valid FFI operator libraries
///
/// Libraries must export a `ffi_operator_magic` symbol that returns this value
/// to be recognized as valid FFI operators.
pub const OPERATOR_MAGIC: u32 = 231123;

/// Function signature for the magic number export
///
/// FFI operator libraries must export this function to be recognized as valid operators.
pub type FFIOperatorMagicFn = extern "C" fn() -> u32;

// =============================
// FFI Return Codes
// =============================

/// FFI return code: Operation succeeded, value found, or iterator has next item
pub const FFI_OK: i32 = 0;

/// FFI return code: Query succeeded but entity doesn't exist
pub const FFI_NOT_FOUND: i32 = 1;

/// FFI return code: Iterator has no more items (alias for FFI_NOT_FOUND)
pub const FFI_END_OF_ITERATION: i32 = 1;

/// FFI error code: Null pointer passed as parameter
pub const FFI_ERROR_NULL_PTR: i32 = -1;

/// FFI error code: Internal error during operation (transaction error, etc.)
pub const FFI_ERROR_INTERNAL: i32 = -2;

/// FFI error code: Memory allocation failed
pub const FFI_ERROR_ALLOC: i32 = -3;

/// FFI error code: Invalid UTF-8 in string parameter
pub const FFI_ERROR_INVALID_UTF8: i32 = -4;

/// FFI error code: Failed to marshal Rust type to FFI struct
pub const FFI_ERROR_MARSHAL: i32 = -5;

// =============================
// Operator Capabilities
// =============================

/// Capability: Operator can process inserts
pub const CAPABILITY_INSERT: u32 = 1 << 0; // 0x01

/// Capability: Operator can process updates
pub const CAPABILITY_UPDATE: u32 = 1 << 1; // 0x02

/// Capability: Operator can process deletes
pub const CAPABILITY_DELETE: u32 = 1 << 2; // 0x04

/// Capability: Operator supports pull(), which is required for join and window flows
pub const CAPABILITY_PULL: u32 = 1 << 3; // 0x08

/// Capability: Operator can drop data without cascading change
pub const CAPABILITY_DROP: u32 = 1 << 4; // 0x10

/// Capability: Operator wants periodic tick() callbacks
pub const CAPABILITY_TICK: u32 = 1 << 5; // 0x20

/// All standard capabilities (Insert + Update + Delete + Pull)
pub const CAPABILITY_ALL_STANDARD: u32 = CAPABILITY_INSERT | CAPABILITY_UPDATE | CAPABILITY_DELETE | CAPABILITY_PULL;

/// Helper to check if a capability is set
///
/// # Example
/// ```
/// use reifydb_flow_operator_abi::*;
///
/// let caps = CAPABILITY_INSERT | CAPABILITY_UPDATE;
/// assert!(has_capability(caps, CAPABILITY_INSERT));
/// assert!(has_capability(caps, CAPABILITY_UPDATE));
/// assert!(!has_capability(caps, CAPABILITY_DELETE));
/// ```
#[inline]
pub const fn has_capability(capabilities: u32, capability: u32) -> bool {
	(capabilities & capability) != 0
}
