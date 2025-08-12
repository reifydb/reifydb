use crate::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, GetHooks, Params, Principal, Transaction,
};
use crate::result::frame::Frame;

pub trait Engine<T>: GetHooks + Send + Sync + Clone + 'static
where
    T: Transaction,
{
    fn begin_command(&self) -> crate::Result<ActiveCommandTransaction<T>>;

    fn begin_query(&self) -> crate::Result<ActiveQueryTransaction<T>>;

    fn command_as(&self, principal: &Principal, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;

    fn query_as(&self, principal: &Principal, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;
}
