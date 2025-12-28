/// Logging callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LogCallbacks {
	/// Log a message
	///
	/// # Parameters
	/// - `operator_id`: Operator ID for identifying the logging operator
	/// - `level`: Log level (0=trace, 1=debug, 2=info, 3=warn, 4=error)
	/// - `message`: Message bytes
	/// - `message_len`: Length of message in bytes
	pub message: extern "C" fn(operator_id: u64, level: u32, message: *const u8, message_len: usize),
}
