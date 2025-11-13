//! FFI-safe type definitions for operator-host communication

use core::ffi::c_void;

/// FFI-safe buffer representing a slice of bytes
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferFFI {
    /// Pointer to the data
    pub ptr: *const u8,
    /// Length of the data
    pub len: usize,
    /// Capacity of the allocated buffer
    pub cap: usize,
}

impl BufferFFI {
    /// Create an empty buffer
    pub const fn empty() -> Self {
        Self {
            ptr: core::ptr::null(),
            len: 0,
            cap: 0,
        }
    }

    /// Create a buffer from a slice
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            ptr: data.as_ptr(),
            len: data.len(),
            cap: data.len(),
        }
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0 || self.ptr.is_null()
    }

    /// Get the buffer as a slice (unsafe - caller must ensure pointer validity)
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.is_empty() {
            &[]
        } else {
            core::slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

/// FFI-safe mutable buffer
#[repr(C)]
#[derive(Debug)]
pub struct MutBufferFFI {
    /// Pointer to the mutable data
    pub ptr: *mut u8,
    /// Length of the data
    pub len: usize,
}

impl MutBufferFFI {
    /// Create an empty mutable buffer
    pub const fn empty() -> Self {
        Self {
            ptr: core::ptr::null_mut(),
            len: 0,
        }
    }
}

/// FFI-safe row representation
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RowFFI {
    /// Row number (unique identifier)
    pub number: u64,
    /// Encoded row data
    pub encoded: BufferFFI,
    /// Opaque layout handle (managed by host)
    pub layout_handle: *const c_void,
}

/// Type of flow diff operation
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDiffType {
    /// Insert a new row
    Insert = 0,
    /// Update an existing row
    Update = 1,
    /// Remove a row
    Remove = 2,
}

/// FFI-safe flow diff
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowDiffFFI {
    /// Type of the diff
    pub diff_type: FlowDiffType,
    /// Previous row state (null for Insert)
    pub pre_row: *const RowFFI,
    /// New row state (null for Remove)
    pub post_row: *const RowFFI,
}

/// FFI-safe flow change containing multiple diffs
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowChangeFFI {
    /// Number of diffs in the change
    pub diff_count: usize,
    /// Pointer to array of diffs
    pub diffs: *const FlowDiffFFI,
    /// Version number for this change
    pub version: u64,
}

impl FlowChangeFFI {
    /// Create an empty flow change
    pub const fn empty() -> Self {
        Self {
            diff_count: 0,
            diffs: core::ptr::null(),
            version: 0,
        }
    }
}

/// FFI-safe collection of rows
#[repr(C)]
#[derive(Debug)]
pub struct RowsFFI {
    /// Number of rows
    pub count: usize,
    /// Pointer to array of row pointers (null entries mean row not found)
    pub rows: *mut *const RowFFI,
}

/// FFI-safe value type matching ReifyDB Value enum
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueTypeFFI {
    /// Value is not defined (null)
    Undefined = 0,
    /// Boolean value
    Boolean = 1,
    /// 4-byte floating point
    Float4 = 2,
    /// 8-byte floating point
    Float8 = 3,
    /// 1-byte signed integer
    Int1 = 4,
    /// 2-byte signed integer
    Int2 = 5,
    /// 4-byte signed integer
    Int4 = 6,
    /// 8-byte signed integer
    Int8 = 7,
    /// 16-byte signed integer
    Int16 = 8,
    /// UTF-8 encoded text
    Utf8 = 9,
    /// 1-byte unsigned integer
    Uint1 = 10,
    /// 2-byte unsigned integer
    Uint2 = 11,
    /// 4-byte unsigned integer
    Uint4 = 12,
    /// 8-byte unsigned integer
    Uint8 = 13,
    /// 16-byte unsigned integer
    Uint16 = 14,
    /// Date value
    Date = 15,
    /// DateTime value
    DateTime = 16,
    /// Time value
    Time = 17,
    /// Duration value
    Duration = 18,
    /// Row number
    RowNumber = 19,
    /// Identity ID (UUID v7)
    IdentityId = 20,
    /// UUID version 4
    Uuid4 = 21,
    /// UUID version 7
    Uuid7 = 22,
    /// Binary large object
    Blob = 23,
    /// Arbitrary-precision signed integer
    Int = 24,
    /// Arbitrary-precision unsigned integer
    Uint = 25,
    /// Arbitrary-precision decimal
    Decimal = 26,
    /// Container that can hold any value type
    Any = 27,
}

/// FFI-safe value data union
///
/// This union holds the actual data for different value types.
/// The specific field to access is determined by the ValueTypeFFI discriminant.
#[repr(C)]
#[derive(Clone, Copy)]
pub union ValueDataFFI {
    /// Boolean value
    pub boolean: bool,
    /// 1-byte signed integer
    pub int1: i8,
    /// 2-byte signed integer
    pub int2: i16,
    /// 4-byte signed integer
    pub int4: i32,
    /// 8-byte signed integer
    pub int8: i64,
    /// 16-byte signed integer (as buffer due to size)
    pub int16: [u8; 16],
    /// 1-byte unsigned integer
    pub uint1: u8,
    /// 2-byte unsigned integer
    pub uint2: u16,
    /// 4-byte unsigned integer
    pub uint4: u32,
    /// 8-byte unsigned integer
    pub uint8: u64,
    /// 16-byte unsigned integer (as buffer due to size)
    pub uint16: [u8; 16],
    /// 4-byte float
    pub float4: f32,
    /// 8-byte float
    pub float8: f64,
    /// Buffer for variable-length data (Utf8, Blob, Int, Uint, Decimal, Any)
    pub buffer: BufferFFI,
    /// Date (as i32 days since epoch)
    pub date: i32,
    /// DateTime (as i64 nanoseconds since epoch)
    pub datetime: i64,
    /// Time (as i64 nanoseconds since midnight)
    pub time: i64,
    /// Duration (as i64 nanoseconds)
    pub duration: i64,
    /// Row number
    pub row_number: u64,
    /// UUID (16 bytes for Uuid4, Uuid7, IdentityId)
    pub uuid: [u8; 16],
}

/// FFI-safe value with type tag
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ValueFFI {
    /// Type of the value
    pub value_type: ValueTypeFFI,
    /// Value data (union)
    pub data: ValueDataFFI,
}

impl ValueFFI {
    /// Create an undefined value
    pub const fn undefined() -> Self {
        Self {
            value_type: ValueTypeFFI::Undefined,
            data: ValueDataFFI { uint8: 0 },
        }
    }

    /// Create a boolean value
    pub const fn boolean(val: bool) -> Self {
        Self {
            value_type: ValueTypeFFI::Boolean,
            data: ValueDataFFI { boolean: val },
        }
    }

    /// Create an int8 value
    pub const fn int8(val: i64) -> Self {
        Self {
            value_type: ValueTypeFFI::Int8,
            data: ValueDataFFI { int8: val },
        }
    }

    /// Create a float8 value
    pub const fn float8(val: f64) -> Self {
        Self {
            value_type: ValueTypeFFI::Float8,
            data: ValueDataFFI { float8: val },
        }
    }

    /// Create a UTF-8 string value
    pub fn utf8(buffer: BufferFFI) -> Self {
        Self {
            value_type: ValueTypeFFI::Utf8,
            data: ValueDataFFI { buffer },
        }
    }

    /// Create a blob value
    pub fn blob(buffer: BufferFFI) -> Self {
        Self {
            value_type: ValueTypeFFI::Blob,
            data: ValueDataFFI { buffer },
        }
    }
}

/// Opaque handle to a transaction (managed by host)
#[repr(C)]
pub struct TransactionHandle {
    _opaque: [u8; 0],
}

/// Opaque handle to an expression (managed by host)
#[repr(C)]
pub struct ExpressionHandle {
    _opaque: [u8; 0],
}

/// Opaque handle to a state iterator (managed by host)
#[repr(C)]
pub struct StateIteratorFFI {
    _opaque: [u8; 0],
}


/// FFI-safe array of values
#[repr(C)]
#[derive(Debug)]
pub struct ValuesFFI {
    /// Number of values
    pub count: usize,
    /// Pointer to array of values
    pub values: *const ValueFFI,
}