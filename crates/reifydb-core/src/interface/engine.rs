use crate::interface::{
    ActiveCommandTransaction, ActiveQueryTransaction, GetHooks, Params, Principal, UnversionedTransaction,
    VersionedTransaction,
};
use crate::result::frame::Frame;

pub trait Engine<VT, UT>: GetHooks + Send + Sync + Clone + 'static
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn begin_command(&self) -> crate::Result<ActiveCommandTransaction<VT, UT>>;

    fn begin_query(&self) -> crate::Result<ActiveQueryTransaction<VT, UT>>;

    fn command_as(&self, principal: &Principal, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;

    fn query_as(&self, principal: &Principal, rql: &str, params: Params) -> crate::Result<Vec<Frame>>;
}
