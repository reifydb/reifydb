use crate::interface::{GetHooks, Principal, Transaction, UnversionedStorage, VersionedStorage};
use crate::result::frame::Frame;
use std::sync::MutexGuard;

pub trait Engine<VS, US, T>: GetHooks + Send + Sync + Clone + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn begin_tx(&self) -> crate::Result<T::Tx>;

    fn begin_rx(&self) -> crate::Result<T::Rx>;

    fn begin_unversioned(&self) -> MutexGuard<US>;
    
    fn tx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;

    fn rx_as(&self, principal: &Principal, rql: &str) -> crate::Result<Vec<Frame>>;
}
