use core::ffi::c_void;

/// Magic number to identify valid FFI operator libraries
///
/// Libraries must export a `ffi_operator_magic` symbol that returns this value
/// to be recognized as valid FFI operators.
pub const OPERATOR_MAGIC: u32 = 231123;

/// Function signature for the magic number export
///
/// FFI operator libraries must export this function to be recognized as valid operators.
pub type OperatorMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating operator instances
pub type OperatorCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
