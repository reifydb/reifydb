use crate::{
	interface::{
		GetHooks, Identity, Params,
		Transaction,
	},
	result::frame::Frame,
	transaction::{StandardCommandTransaction, StandardQueryTransaction},
};

pub trait Engine<T: Transaction>:
	GetHooks + Send + Sync + Clone + 'static
{
	fn begin_command(&self) -> crate::Result<StandardCommandTransaction<T>>;

	fn begin_query(&self) -> crate::Result<StandardQueryTransaction<T>>;

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
