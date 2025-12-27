use super::FlowDiffFFI;

/// FFI-safe representation of flow change origin
///
/// Encodes both Internal and External origins:
/// - origin: 0 = Internal, 1 = External.Table, 2 = External.View, 3 = External.TableVirtual, 4 = External.RingBuffer
/// - id: For Internal, this is the FlowNodeId. For External, this is the source ID (TableId, ViewId, etc.)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowOriginFFI {
	pub origin: u8,
	pub id: u64,
}

/// FFI-safe flow change containing multiple diffs
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FlowChangeFFI {
	/// Origin of this change
	pub origin: FlowOriginFFI,
	/// Number of diffs in the change
	pub diff_count: usize,
	/// Pointer to array of diffs
	pub diffs: *const FlowDiffFFI,
	/// Version number for this change
	pub version: u64,
}

impl FlowChangeFFI {
	/// Create an empty flow change with Internal origin 0
	pub const fn empty() -> Self {
		Self {
			origin: FlowOriginFFI {
				origin: 0,
				id: 0,
			},
			diff_count: 0,
			diffs: core::ptr::null(),
			version: 0,
		}
	}
}
