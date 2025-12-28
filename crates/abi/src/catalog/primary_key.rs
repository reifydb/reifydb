/// FFI-safe primary key definition
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PrimaryKeyFFI {
	/// Primary key ID (u64)
	pub id: u64,
	/// Number of columns in primary key
	pub column_count: usize,
	/// Array of column IDs that compose the primary key
	pub column_ids: *const u64,
}
