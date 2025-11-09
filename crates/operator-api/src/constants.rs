//! Constants and version information for the FFI operator API

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API_VERSION: u32 = 1;

/// Minimum supported API version
///
/// Operators with API versions below this will not be loaded.
pub const MIN_API_VERSION: u32 = 1;

/// Maximum supported API version
///
/// Operators with API versions above this will not be loaded.
pub const MAX_API_VERSION: u32 = 1;

// ==================== Capability Flags ====================
// These flags are informational and help the host optimize operator execution.
// They do not restrict functionality - all operators have access to all features.

/// Operator uses state management functions
pub const CAP_USES_STATE: u32 = 1 << 0;

/// Operator uses keyed state (multiple keys)
pub const CAP_KEYED_STATE: u32 = 1 << 1;

/// Operator evaluates expressions
pub const CAP_USES_EXPRESSIONS: u32 = 1 << 2;

/// Operator uses batch operations
pub const CAP_BATCH: u32 = 1 << 3;

/// Operator supports windowing
pub const CAP_WINDOWED: u32 = 1 << 4;

/// Operator is deterministic (same input always produces same output)
pub const CAP_DETERMINISTIC: u32 = 1 << 5;

/// Operator modifies row data
pub const CAP_MUTATES_ROWS: u32 = 1 << 6;

// ==================== Log Levels ====================

/// Trace level logging
pub const LOG_TRACE: u32 = 0;

/// Debug level logging
pub const LOG_DEBUG: u32 = 1;

/// Info level logging
pub const LOG_INFO: u32 = 2;

/// Warning level logging
pub const LOG_WARN: u32 = 3;

/// Error level logging
pub const LOG_ERROR: u32 = 4;

// ==================== Standard Symbols ====================

/// Symbol name for getting operator descriptor
pub const SYMBOL_GET_DESCRIPTOR: &[u8] = b"get_operator_descriptor\0";

/// Symbol name for creating operator instance
pub const SYMBOL_CREATE_OPERATOR: &[u8] = b"create_operator\0";

/// Symbol name for getting API version
pub const SYMBOL_GET_API_VERSION: &[u8] = b"get_api_version\0";