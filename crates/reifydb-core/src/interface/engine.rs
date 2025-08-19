use crate::{
	interface::{
		CommandTransaction, GetHooks, Identity, Params,
		QueryTransaction, Transaction,
	},
	result::frame::Frame,
};

pub trait Engine<T: Transaction>:
	GetHooks + Send + Sync + Clone + 'static
{
	fn begin_command(&self) -> crate::Result<CommandTransaction<T>>;

	fn begin_query(&self) -> crate::Result<QueryTransaction<T>>;

	fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>>;

	fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>>;
}
