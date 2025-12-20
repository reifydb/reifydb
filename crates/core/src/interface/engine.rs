use crate::{
	interface::{CommandTransaction, Identity, Params, QueryTransaction, WithEventBus},
	stream::SendableFrameStream,
};

/// Async database engine trait.
///
/// This trait defines the core interface for database engines. The `command_as`
/// and `query_as` methods return async streams for non-blocking execution.
///
/// Note: The associated transaction types don't require `Send` because they
/// are used entirely within blocking tasks. The engine itself must be `Send + Sync`
/// to be cloned into async tasks.
pub trait Engine: WithEventBus + Send + Sync + Clone + 'static {
	type Command: CommandTransaction;
	type Query: QueryTransaction;

	/// Begin a new command (write) transaction.
	fn begin_command(&self) -> crate::Result<Self::Command>;

	/// Begin a new query (read) transaction.
	fn begin_query(&self) -> crate::Result<Self::Query>;

	/// Execute a command and return a stream of result frames.
	///
	/// Commands are write operations (INSERT, UPDATE, DELETE, DDL) that modify
	/// the database state. The command runs in a transaction that is automatically
	/// committed on success or rolled back on error.
	fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> SendableFrameStream;

	/// Execute a query and return a stream of result frames.
	///
	/// Queries are read operations (SELECT) that do not modify the database.
	/// Results are streamed as they become available, providing backpressure
	/// if the consumer is slow.
	fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> SendableFrameStream;
}
