use crate::interface::{
    GetHooks, Principal, UnversionedStorage, UnversionedTransaction, VersionedStorage,
    VersionedTransaction,
};
use crate::result::frame::Frame;

pub trait Engine<VS, US, T, UT>: GetHooks + Send + Sync + Clone + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    fn begin_write(&self) -> crate::Result<T::Write>;

    fn begin_read(&self) -> crate::Result<T::Read>;

    fn write_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;

    fn read_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;
}
