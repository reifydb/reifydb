use crate::interface::{Transaction, UnversionedStorage, VersionedStorage};

pub trait Engine<VS, US, T>: Send + Sync + 'static
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn begin_tx(&self) -> crate::Result<T::Tx>;

    fn begin_rx(&self) -> crate::Result<T::Rx>;

    // fn tx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Self::Frame>>;
    //
    // fn rx_as(&self, _principal: &Principal, rql: &str) -> crate::Result<Vec<Self::Frame>>;
}
