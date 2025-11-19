//! Constants and version information for the FFI operator API

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API_VERSION: u32 = 1;

/// Magic number to identify valid FFI operator libraries
///
/// Libraries must export a `ffi_operator_magic` symbol that returns this value
/// to be recognized as valid FFI operators.
pub const OPERATOR_MAGIC: u32 = 231123;

/// Function signature for the magic number export
///
/// FFI operator libraries must export this function to be recognized as valid operators.
pub type FFIOperatorMagicFn = extern "C" fn() -> u32;
