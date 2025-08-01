use crate::interface::{
    ActiveReadTransaction, ActiveWriteTransaction, GetHooks, Principal, UnversionedTransaction,
    VersionedTransaction,
};
use crate::result::frame::Frame;

pub trait Engine<VT, UT>: GetHooks + Send + Sync + Clone + 'static
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn begin_write(&self) -> crate::Result<ActiveWriteTransaction<VT, UT>>;

    fn begin_read(&self) -> crate::Result<ActiveReadTransaction<VT, UT>>;

    fn write_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;

    fn read_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;
}
