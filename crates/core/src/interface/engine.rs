use crate::{
	interface::{CommandTransaction, Identity, Params, QueryTransaction, WithEventBus},
	value::frame::Frame,
};

pub trait Engine: WithEventBus + Send + Sync + Clone + 'static {
	type Command: CommandTransaction;
	type Query: QueryTransaction;

	fn begin_command(&self) -> crate::Result<Self::Command>;

	fn begin_query(&self) -> crate::Result<Self::Query>;

	fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;

	fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;
}
