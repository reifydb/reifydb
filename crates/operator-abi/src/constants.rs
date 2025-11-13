//! Constants and version information for the FFI operator API

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API_VERSION: u32 = 1;